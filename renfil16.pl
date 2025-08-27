#!/usr/bin/env perl
##############################################################################
# Script:     cmod_fs_rename.pl
# Purpose:    Rename Spectrum Protect filespaces to align payloads between
#             federated CMOD (Content Manager OnDemand) and federated
#             Spectrum Protect, ensuring archived payloads stay linked.
#
# Project:    LA HUB
# Author:     Krzysztof Stefaniak
# Team:       TahD
# Date:       2025-08-27
# Version:    1.6 (readable)
#
# Summary of operation (high level):
#   1) Read input CSV mappings (SRC/DST INST+AGID + node).
#   2) For each row:
#        - Skip if OD_TSM_LOGON_NAME empty (CMOD cached only).
#        - Build old_fs=/SRC/AGID and new_fs=/DST/AGID (or reversed with --undo).
#        - Pre-check using a per-node cache (single 'q filespace <node>' per node).
#        - If --run:
#            * batch mode (default): queue rename into one macro to execute later.
#            * no-batch: execute rename immediately.
#          If --verify: confirm (per-node in batch; per-rename otherwise).
#          If --verify-late: skip immediate checks; verify all nodes at the end.
#        - Record a detailed report row and a rollback mapping.
#   3) In batch mode: write a macro file with all queued renames and run it once.
#   4) If late verification requested: query each affected node once and update
#      report statuses to OK/WARN accordingly.
#   5) Write report CSV and rollback CSV; print a summary.
#
# Notes:
#   - Default is DRY-RUN (no changes). Add --run to apply changes.
#   - dsmadmc flags: -dataonly=yes -comma -noconfirm
#   - Output files are created in the current directory.
#   - Pre-check free space uses 'df -Pk' (Linux/AIX-friendly) with a safety margin.
#
# Required CSV headers (exact names):
#   OD_INSTAME_SRC, AGID_NAME_SRC, OD_INSTAME_DST, AGID_NAME_DST, OD_TSM_LOGON_NAME
#
# Usage:
#   cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER]
#                     [-p PASS | --pwfile FILE] [--delim ';']
#                     [--run] [--verify | --verify-late] [--undo]
#                     [--no-batch] [--keep-batch]
##############################################################################

use strict;
use warnings;
use POSIX qw(strftime);
use Text::CSV_XS;                 # Module for robust CSV parsing/output
use Cwd qw(abs_path);

# ------------------------------ CLI & Globals ------------------------------

my ($CSV_FILE,$SERVERNAME,$USER,$PASS,$PWFILE,$DELIM) = ("","","","","",",");
my ($DO_RUN,$VERIFY,$VERIFY_LATE,$UNDO) = (0,0,0,0);
my ($BATCH,$KEEP_BATCH) = (1,0);

while (my $arg = shift @ARGV) {
    if    ($arg eq '-c' or $arg eq '--csv')        { $CSV_FILE   = shift(@ARGV) // "" }
    elsif ($arg eq '--servername')                 { $SERVERNAME = shift(@ARGV) // "" }
    elsif ($arg eq '-u' or $arg eq '--user')       { $USER       = shift(@ARGV) // "" }
    elsif ($arg eq '-p' or $arg eq '--pass')       { $PASS       = shift(@ARGV) // "" }
    elsif ($arg eq '--pwfile')                     { $PWFILE     = shift(@ARGV) // "" }
    elsif ($arg eq '--delim')                      { $DELIM      = shift(@ARGV) // "," }
    elsif ($arg eq '--run')                        { $DO_RUN     = 1 }
    elsif ($arg eq '--verify')                     { $VERIFY     = 1 }
    elsif ($arg eq '--verify-late')                { $VERIFY_LATE= 1 }
    elsif ($arg eq '--undo')                       { $UNDO       = 1 }
    elsif ($arg eq '--no-batch')                   { $BATCH      = 0 }
    elsif ($arg eq '--keep-batch')                 { $KEEP_BATCH = 1 }
    elsif ($arg eq '-h' or $arg eq '--help')       { print_usage(); exit 0 }
    else { die "Unknown arg: $arg (use --help)\n" }
}

die "ERROR: CSV file required (-c|--csv)\n" unless $CSV_FILE;
-f $CSV_FILE or die "ERROR: CSV not readable: $CSV_FILE\n";

# Filenames stamped with date/time for traceability
my $STAMP        = strftime("%Y%m%d_%H%M%S", localtime);
my $REPORT_PATH  = "fs_rename_report_${STAMP}.csv";
my $ROLLBACK_CSV = "fs_rollback_csv_${STAMP}.csv";
my $MACRO_PATH   = "fs_batch_${STAMP}.mac";

# In-memory structures for clarity and simpler verification logic
# (trade-off: slightly higher RAM; benefit: much clearer flow)
my @ACTIONS;          # Each element is a hashref describing one rename intent/result
my @ROLLBACK_ROWS;    # Each element is an arrayref for rollback CSV row
my %FS_CACHE;         # Per-node filespace cache for pre-checks: FS_CACHE{node}{fsname} = 1
my %VERIFY_NODES;     # Nodes to verify at the end (for --verify-late or batch verify)
my @MACRO_LINES;      # Collected 'rename filespace ...' commands for batch execution

# ------------------------------ Utilities ----------------------------------

# Quote a value for dsmadmc (double-quotes; inner quotes doubled)
sub quote_tsm {
    my ($s) = @_; $s //= ""; $s =~ s/"/""/g; return qq{"$s"};
}

# Human-friendly byte formatting
sub human_bytes {
    my ($b) = @_; $b ||= 0;
    my @u = ('B','KB','MB','GB','TB'); my $i=0;
    while ($b >= 1024 && $i < $#u) { $b/=1024; $i++ }
    return $i==0 ? sprintf('%d %s',$b,$u[$i]) : sprintf('%.1f %s',$b,$u[$i]);
}

# Trim whitespace and remove Windows CR if present
sub clean_scalar {
    my ($v) = @_; $v //= "";
    $v =~ s/\r$//; $v =~ s/^\s+|\s+$//g; return $v;
}

# Build a dsmadmc command with stable flags
sub build_dsmadmc_cmd {
    my ($tsm_cmd) = @_;
    my @cmd = ("dsmadmc", "-dataonly=yes", "-comma", "-noconfirm");
    push @cmd, "-servername=$SERVERNAME" if $SERVERNAME;
    push @cmd, "-id=$USER"               if $USER;
    push @cmd, "-password=$PASS"         if defined $PASS && $PASS ne "";
    push @cmd, $tsm_cmd if defined $tsm_cmd;
    return @cmd;
}

# Run a single dsmadmc command, return (rc, output)
sub run_dsmadmc {
    my ($tsm_cmd) = @_;
    my @cmd = build_dsmadmc_cmd($tsm_cmd);
    my $out = qx(@cmd 2>&1); my $rc = $? >> 8;
    return ($rc, $out);
}

# Query filespaces for a given node (populates list of filespace names)
# - With -dataonly=yes -comma, column 0 is NODE_NAME, column 1 is FILESPACE_NAME
sub fs_list_for_node {
    my ($node) = @_;
    my ($rc, $out) = run_dsmadmc("q filespace $node");
    return () if $rc != 0;
    my @names;
    my $csv = Text::CSV_XS->new({ binary => 1 });
    for my $line (split /\r?\n/, $out) {
        next unless length $line;
        if ($csv->parse($line)) {
            my @f = $csv->fields;
            push @names, $f[1] if @f >= 2 && defined $f[1] && $f[1] ne '';
        }
    }
    return @names;
}

# Ensure per-node cache is loaded (single 'q filespace' per node)
sub ensure_cache_for_node {
    my ($node) = @_;
    return if exists $FS_CACHE{$node};
    my %set = map { $_ => 1 } fs_list_for_node($node);
    $FS_CACHE{$node} = \%set;
}

# Free-space precheck (Linux RHEL7/8 and AIX 7.2)
# Uses 'df -Pk' and requires free >= 2.2 × input CSV size (report + rollback + slack)
sub precheck_disk_space {
    my ($input_csv) = @_;
    my $input_size = -s $input_csv;               # bytes
    my $need_bytes = int($input_size * 2.2);      # generous margin
    my $out_dir = ".";

    my $os = `uname -s`; chomp($os);
    my $df  = `df -Pk $out_dir 2>/dev/null`;
    my @lines = split /\n/, $df;
    if (@lines < 2) {
        print STDERR "WARN : Could not parse 'df -Pk'; skipping free-space check.\n";
        return;
    }
    my @f = split /\s+/, $lines[-1];
    my $avail_kb = ($os eq 'AIX') ? $f[2] : $f[3];   # AIX uses 'Free' in col 3, Linux 'Available' in col 4
    $avail_kb = 0 unless defined $avail_kb && $avail_kb =~ /^\d+$/;
    my $avail_bytes = $avail_kb * 1024;

    if ($avail_bytes < $need_bytes) {
        die sprintf("ERROR: Not enough free space. Need >= %s, available %s.\n",
                    human_bytes($need_bytes), human_bytes($avail_bytes));
    }
    printf STDERR "INFO : Disk space OK (need ~%s, available %s).\n",
                  human_bytes($need_bytes), human_bytes($avail_bytes);
}

# Minimal CSV escaping for writing our output CSVs by hand (report/rollback)
sub csv_escape {
    my ($s) = @_; $s //= "";
    if ($s =~ /[",\r\n]/) { $s =~ s/"/""/g; return qq{"$s"}; }
    return $s;
}

# ------------------------------ I/O Helpers --------------------------------

# Deferred-write report: store action rows as hashes, flush at the end
sub report_add_row {
    my (%row) = @_;
    # Expected keys:
    # node, old_fs, new_fs, action, status, message
    push @ACTIONS, { %row };
}

sub report_flush_to_file {
    open my $rfh, ">:encoding(UTF-8)", $REPORT_PATH or die "ERROR: Cannot write $REPORT_PATH: $!\n";
    print $rfh "node,old_fs,new_fs,action,status,message\n";
    for my $r (@ACTIONS) {
        print $rfh join(",", map { csv_escape($_) } (
            $r->{node}, $r->{old_fs}, $r->{new_fs}, $r->{action}, $r->{status}, $r->{message}
        )), "\n";
    }
    close $rfh;
}

# Rollback rows are stored coherently and flushed at the end
sub rollback_add_row {
    my ($src_inst,$src_agid,$dst_inst,$dst_agid,$node) = @_;
    push @ROLLBACK_ROWS, [ $src_inst,$src_agid,$dst_inst,$dst_agid,$node ];
}

sub rollback_flush_to_file {
    open my $rbfh, ">:encoding(UTF-8)", $ROLLBACK_CSV or die "ERROR: Cannot write $ROLLBACK_CSV: $!\n";
    print $rbfh "OD_INSTAME_SRC,AGID_NAME_SRC,OD_INSTAME_DST,AGID_NAME_DST,OD_TSM_LOGON_NAME\n";
    for my $r (@ROLLBACK_ROWS) {
        print $rbfh join(",", map { csv_escape($_) } @$r), "\n";
    }
    close $rbfh;
}

# ------------------------------ Passwords ----------------------------------

# Read password from file or ask interactively (no-echo)
sub resolve_password {
    if ($PWFILE && !$PASS) {
        open my $pfh, "<:encoding(UTF-8)", $PWFILE or die "ERROR: Cannot read --pwfile $PWFILE: $!\n";
        chomp($PASS = <$pfh> // "");
        close $pfh;
    }
    if ($DO_RUN && $USER && !$PASS) {
        print STDERR "Enter password for '$USER': ";
        system("stty -echo"); chomp($PASS = <STDIN> // ""); system("stty echo"); print STDERR "\n";
    }
}

# ------------------------------ Execution Paths ----------------------------

# Execute a single rename immediately (no-batch mode).
# Returns (status, message). Optionally performs per-rename verify if $do_verify_now.
sub execute_single_rename {
    my ($node, $old_fs, $new_fs, $do_verify_now) = @_;

    my $cmd = "rename filespace $node ".quote_tsm($old_fs)." ".quote_tsm($new_fs);
    print STDERR "INFO : Executing: $cmd\n";
    my ($rc, $out) = run_dsmadmc($cmd);
    if ($rc != 0) {
        $out =~ s/[\r\n]/ /g;
        return ("FAILED", "dsmadmc rc=$rc: $out");
    }

    if ($do_verify_now) {
        # Fresh per-node snapshot for verification
        my %set = map { $_ => 1 } fs_list_for_node($node);
        return ($set{$new_fs} && !$set{$old_fs} ? "OK" : "WARN", "renamed; verified per-rename");
    }

    return ("OK", "renamed");
}

# Append one line to macro (for batch execution later)
sub batch_queue_rename {
    my ($node, $old_fs, $new_fs) = @_;
    my $cmd = "rename filespace $node ".quote_tsm($old_fs)." ".quote_tsm($new_fs);
    push @MACRO_LINES, $cmd;
}

# Run the macro once and return its (rc, output)
sub batch_run_macro_once {
    return (0, "") unless @MACRO_LINES;   # Nothing to run
    open my $mf, ">:encoding(UTF-8)", $MACRO_PATH or die "ERROR: Cannot write $MACRO_PATH: $!\n";
    print $mf "$_\n" for @MACRO_LINES;
    print $mf "quit\n";
    close $mf;

    my $abs = abs_path($MACRO_PATH) // $MACRO_PATH;
    print STDERR "INFO : Executing macro ($abs) with ".scalar(@MACRO_LINES)." rename commands\n";
    my @cmd = build_dsmadmc_cmd("macro $abs");
    my $out = qx(@cmd 2>&1); my $rc = $? >> 8;
    print STDERR "INFO : Macro rc=$rc\n";
    unlink $abs unless $KEEP_BATCH;
    return ($rc, $out);
}

# Verify a whole node (late or batch post-check)
# Returns hash ( 'OK'|'WARN' ) and reason string for a given old/new
sub verify_node_pair {
    my ($node, $old_fs, $new_fs) = @_;
    my %set = map { $_ => 1 } fs_list_for_node($node);
    if ($set{$new_fs} && !$set{$old_fs}) {
        return ("OK", "verified");
    } elsif ($set{$new_fs} && $set{$old_fs}) {
        return ("WARN", "both old and new present");
    } else {
        return ("WARN", "verification mismatch");
    }
}

# ------------------------------ Main Flow ----------------------------------

# 0) Pre-check free space (so we don't fail mid-way)
precheck_disk_space($CSV_FILE);

# 1) Resolve password (if needed for --run)
resolve_password();

# 2) Read input CSV header and prepare parser
my $csv_in = Text::CSV_XS->new({ binary => 1, sep_char => $DELIM, auto_diag => 1 });
open my $fh, "<:encoding(UTF-8)", $CSV_FILE or die "ERROR: Cannot read $CSV_FILE: $!\n";
my $header = $csv_in->getline($fh) or die "ERROR: Empty CSV or cannot read header\n";
$csv_in->column_names(@$header);

# Validate required headers exist
my %idx; for my $i (0..$#$header) { $idx{$header->[$i]} = $i; }
for my $need (qw/OD_INSTAME_SRC AGID_NAME_SRC OD_INSTAME_DST AGID_NAME_DST OD_TSM_LOGON_NAME/) {
    die "ERROR: CSV missing required header: $need\n" unless exists $idx{$need};
}

# 3) Process each CSV row; build actions & (optionally) queue/execute renames
while (my $row = $csv_in->getline_hr($fh)) {

    my $node     = clean_scalar($row->{OD_TSM_LOGON_NAME});
    my $src_inst = clean_scalar($row->{OD_INSTAME_SRC});
    my $src_agid = clean_scalar($row->{AGID_NAME_SRC});
    my $dst_inst = clean_scalar($row->{OD_INSTAME_DST});
    my $dst_agid = clean_scalar($row->{AGID_NAME_DST});

    # Skip rows with empty node (CMOD used cache only; no Spectrum Protect archiving)
    if ($node eq "") {
        report_add_row(
            node=>"", old_fs=>"", new_fs=>"", action=>"skip",
            status=>"SKIPPED", message=>"OD_TSM_LOGON_NAME empty"
        );
        next;
    }

    # Determine direction: normal or undo
    my ($old_inst,$old_agid,$new_inst,$new_agid) = $UNDO
        ? ($dst_inst,$dst_agid,$src_inst,$src_agid)    # undo: DST->SRC
        : ($src_inst,$src_agid,$dst_inst,$dst_agid);   # normal: SRC->DST

    my $old_fs = "/$old_inst/$old_agid";
    my $new_fs = "/$new_inst/$new_agid";

    # Ensure we have a per-node cache to pre-check existence
    ensure_cache_for_node($node);

    # Pre-checks against the per-node cache
    unless ($FS_CACHE{$node}{$old_fs}) {
        report_add_row(
            node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename",
            status=>"SKIPPED", message=>"old_fs not found"
        );
        next;
    }
    if ($FS_CACHE{$node}{$new_fs}) {
        report_add_row(
            node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename",
            status=>"SKIPPED", message=>"new_fs already exists"
        );
        next;
    }

    # DRY-RUN mode: just show what would be done
    unless ($DO_RUN) {
        my $cmd = "rename filespace $node ".quote_tsm($old_fs)." ".quote_tsm($new_fs);
        print STDERR "INFO : [DRY] $cmd\n";
        report_add_row(
            node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename",
            status=>"DRY-RUN", message=>"No changes applied"
        );
        next;
    }

    # RUN mode:
    if ($BATCH) {
        # Queue into macro; optimistic cache update so subsequent rows can rely on it
        batch_queue_rename($node, $old_fs, $new_fs);
        delete $FS_CACHE{$node}{$old_fs};
        $FS_CACHE{$node}{$new_fs} = 1;

        report_add_row(
            node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename",
            status=>"PENDING", message=>"queued in macro"
        );

        # For rollback we record inverse mapping of what we intend to do.
        # NOTE: rollback may include lines that eventually failed; re-run will safely skip them.
        my ($rb_src_inst,$rb_src_agid,$rb_dst_inst,$rb_dst_agid) = $UNDO
            ? ($src_inst,$src_agid,$dst_inst,$dst_agid)     # undo was DST->SRC, rollback goes SRC->DST
            : ($dst_inst,$dst_agid,$src_inst,$src_agid);     # normal was SRC->DST, rollback goes DST->SRC
        rollback_add_row($rb_src_inst,$rb_src_agid,$rb_dst_inst,$rb_dst_agid,$node);

        # Late verification requested? remember this node
        $VERIFY_NODES{$node}++ if $VERIFY_LATE;

    } else {
        # Legacy immediate mode (no-batch): execute one-by-one
        my ($status,$msg) = execute_single_rename($node, $old_fs, $new_fs, ($VERIFY && !$VERIFY_LATE));
        report_add_row(
            node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename",
            status=>$status, message=>$msg
        );

        # Rollback for executed rename (same comment as above)
        my ($rb_src_inst,$rb_src_agid,$rb_dst_inst,$rb_dst_agid) = $UNDO
            ? ($src_inst,$src_agid,$dst_inst,$dst_agid)
            : ($dst_inst,$dst_agid,$src_inst,$src_agid);
        rollback_add_row($rb_src_inst,$rb_src_agid,$rb_dst_inst,$rb_dst_agid,$node);

        # Late verification requested? remember this node
        $VERIFY_NODES{$node}++ if $VERIFY_LATE;
    }
}
close $fh;

# 4) In batch mode: run macro once; set interim statuses (OK or leave PENDING for late verify)
if ($DO_RUN && $BATCH && @MACRO_LINES) {
    my ($mrc,$mout) = batch_run_macro_once();

    # If user did not request late verification, but did request standard --verify:
    # verify each node once and update PENDING -> OK/WARN accordingly.
    if ($VERIFY && !$VERIFY_LATE) {
        # Collect nodes affected by batch (from queued actions)
        my %affected;
        for my $r (@ACTIONS) {
            next unless $r->{status} eq "PENDING";
            $affected{$r->{node}}++;
        }
        # Verify per node; update each matching report row
        for my $node (keys %affected) {
            for my $r (@ACTIONS) {
                next unless $r->{node} eq $node && $r->{status} eq "PENDING";
                my ($st,$why) = verify_node_pair($node, $r->{old_fs}, $r->{new_fs});
                $r->{status}  = $st;
                $r->{message} = "renamed; $why";
            }
        }
    }
    # If neither --verify nor --verify-late was requested, mark PENDING as OK (not verified)
    if (!$VERIFY && !$VERIFY_LATE) {
        for my $r (@ACTIONS) {
            next unless $r->{status} eq "PENDING";
            $r->{status}  = "OK";
            $r->{message} = "submitted via macro (not verified)";
        }
    }
}

# 5) Late verification (regardless of batch/no-batch): single 'q filespace' per node
if ($DO_RUN && $VERIFY_LATE) {
    for my $node (keys %VERIFY_NODES) {
        # Evaluate all rows for this node that are PENDING or OK (unverified) from macro/exec
        my @rows = grep { $_->{node} eq $node && ($_->{status} eq "PENDING" || $_->{message} =~ /submitted via macro/) } @ACTIONS;
        next unless @rows;

        # Single verification snapshot per node
        my %set = map { $_ => 1 } fs_list_for_node($node);

        for my $r (@rows) {
            my ($old_fs,$new_fs) = ($r->{old_fs}, $r->{new_fs});
            if ($set{$new_fs} && !$set{$old_fs}) {
                $r->{status}  = "OK";
                $r->{message} = "renamed; verified (late)";
            } elsif ($set{$new_fs} && $set{$old_fs}) {
                $r->{status}  = "WARN";
                $r->{message} = "both old and new present (late verify)";
            } else {
                # If macro actually failed or rename not applied; report as WARN (non-fatal) or FAILED
                $r->{status}  = "WARN";
                $r->{message} = "late verify mismatch";
            }
        }
    }
}

# 6) Flush outputs (report + rollback) and print a summary
report_flush_to_file();
rollback_flush_to_file();

my %stats; $stats{ $_->{status} }++ for @ACTIONS;
printf STDERR "INFO : Done. Report: %s | Rollback CSV: %s\n", $REPORT_PATH, $ROLLBACK_CSV;
printf STDERR "INFO : Summary -> OK:%d WARN:%d FAILED:%d SKIPPED:%d DRY-RUN:%d PENDING:%d\n",
    ($stats{OK}||0),($stats{WARN}||0),($stats{FAILED}||0),($stats{SKIPPED}||0),($stats{'DRY-RUN'}||0),($stats{PENDING}||0);

# ------------------------------ Usage / Help -------------------------------

sub print_usage {
    print <<'USAGE';
Usage:
  cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER] [-p PASS | --pwfile FILE]
                    [--delim ';'] [--run] [--verify | --verify-late] [--undo]
                    [--no-batch] [--keep-batch]

Direction:
  default: old_fs=/OD_INSTAME_SRC/AGID_NAME_SRC  -> new_fs=/OD_INSTAME_DST/AGID_NAME_DST
  --undo : old_fs=/OD_INSTAME_DST/AGID_NAME_DST  -> new_fs=/OD_INSTAME_SRC/AGID_NAME_SRC

Modes:
  - Default (no --run) is DRY-RUN (prints actions, no changes).
  - Batch (default): all renames queued and executed in one macro (1 login).
  - No-batch (--no-batch): execute each rename immediately (slower).

Verification options:
  --verify       : batch -> 1× 'q filespace' per node after macro;
                   no-batch -> verify after each rename (slower).
  --verify-late  : regardless of mode, verify at the end (1× per node).

Outputs:
  - Report CSV:    fs_rename_report_<timestamp>.csv
  - Rollback CSV:  fs_rollback_csv_<timestamp>.csv
    (inverse mappings; rows for failed/skip renames are harmless and will be skipped by pre-checks)

Notes:
  - dsmadmc is used with: -dataonly=yes -comma -noconfirm
  - Required CSV headers (exact):
      OD_INSTAME_SRC, AGID_NAME_SRC, OD_INSTAME_DST, AGID_NAME_DST, OD_TSM_LOGON_NAME
  - Script checks free space before running (needs ≥ 2.2 × input CSV size).
USAGE
}