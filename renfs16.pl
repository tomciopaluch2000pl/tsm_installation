#!/usr/bin/env perl
##############################################################################
# Script:     cmod_fs_rename.pl
# Purpose:    Batch-rename Spectrum Protect filespaces to keep CMOD payloads
#             correctly linked after federation (CMOD ↔ Spectrum Protect).
#
# Project:    LA HUB
# Author:     Krzysztof Stefaniak
# Team:       TahD
# Date:       2025-08-27
# Version:    1.6 (final, batch-only, readable version)
#
# How it works:
#   - Reads a CSV with mappings (headers listed below).
#   - For each row builds:
#       old_fs=/OD_INSTAME_SRC/AGID_NAME_SRC
#       new_fs=/OD_INSTAME_DST/AGID_NAME_DST
#     (With --undo the direction is reversed.)
#   - For each node: loads a single 'q filespace <node>' (cached).
#   - With --run: queues all valid renames into one dsmadmc macro.
#   - With --verify: after macro performs one 'q filespace <node>' per node.
#   - Produces a detailed report CSV and a rollback CSV.
#
# Required CSV headers:
#   OD_INSTAME_SRC, AGID_NAME_SRC, OD_INSTAME_DST, AGID_NAME_DST, OD_TSM_LOGON_NAME
##############################################################################

use strict;
use warnings;
use POSIX qw(strftime);
use Text::CSV_XS;
use Cwd qw(abs_path);

# ----------------------------- CLI / Settings ------------------------------

my ($CSV_FILE,$SERVERNAME,$USER,$PASS,$PWFILE,$DELIM) = ("","","","","",",");
my ($DO_RUN,$VERIFY,$UNDO,$KEEP_BATCH) = (0,0,0,0);

while (my $arg = shift @ARGV) {
    if    ($arg eq '-c' or $arg eq '--csv') { $CSV_FILE   = shift(@ARGV) // "" }
    elsif ($arg eq '--servername')          { $SERVERNAME = shift(@ARGV) // "" }
    elsif ($arg eq '-u' or $arg eq '--user'){ $USER       = shift(@ARGV) // "" }
    elsif ($arg eq '-p' or $arg eq '--pass'){ $PASS       = shift(@ARGV) // "" }
    elsif ($arg eq '--pwfile')              { $PWFILE     = shift(@ARGV) // "" }
    elsif ($arg eq '--delim')               { $DELIM      = shift(@ARGV) // "," }
    elsif ($arg eq '--run')                 { $DO_RUN     = 1 }
    elsif ($arg eq '--verify')              { $VERIFY     = 1 }
    elsif ($arg eq '--undo')                { $UNDO       = 1 }
    elsif ($arg eq '--keep-batch')          { $KEEP_BATCH = 1 }
    elsif ($arg eq '-h' or $arg eq '--help'){ print_usage(); exit 0 }
    else { die "Unknown arg: $arg (use --help)\n" }
}

die "ERROR: CSV file required (-c|--csv)\n" unless $CSV_FILE;
-f $CSV_FILE or die "ERROR: CSV not readable: $CSV_FILE\n";

# ------------------------------- Filenames ---------------------------------

my $STAMP        = strftime("%Y%m%d_%H%M%S", localtime);
my $REPORT_PATH  = "fs_rename_report_${STAMP}.csv";
my $ROLLBACK_CSV = "fs_rollback_csv_${STAMP}.csv";
my $MACRO_PATH   = "fs_batch_${STAMP}.mac";

# ------------------------------- State -------------------------------------

my @ACTIONS;        # Report rows
my @ROLLBACK_ROWS;  # Rollback rows
my %FS_CACHE;       # Cache of filespaces per node
my %VERIFY_NODES;   # Nodes to verify
my @MACRO_LINES;    # Macro commands

# ------------------------------ Utilities ----------------------------------

# Quote for TSM command line
sub quote_tsm {
    my ($s) = @_;
    $s //= "";
    $s =~ s/"/""/g;
    return qq{"$s"};
}

# Convert bytes into human readable units
sub human_bytes {
    my ($bytes) = @_;
    $bytes ||= 0;
    my @units = qw(B KB MB GB TB);
    my $i = 0;

    while ($bytes >= 1024 && $i < $#units) {
        $bytes /= 1024;
        $i++;
    }

    if ($i == 0) {
        return sprintf("%d %s", $bytes, $units[$i]);
    }
    else {
        return sprintf("%.1f %s", $bytes, $units[$i]);
    }
}

# Trim CR and whitespace
sub clean_scalar {
    my ($v) = @_;
    $v //= "";
    $v =~ s/\r$//;
    $v =~ s/^\s+|\s+$//g;
    return $v;
}

# Escape CSV value
sub csv_escape {
    my ($s) = @_;
    $s //= "";
    if ($s =~ /[",\r\n]/) {
        $s =~ s/"/""/g;
        return qq{"$s"};
    }
    return $s;
}

# Build dsmadmc command line
sub build_dsmadmc_cmd {
    my ($tsm_cmd) = @_;
    my @cmd = ("dsmadmc","-dataonly=yes","-comma","-noconfirm");

    push @cmd, "-servername=$SERVERNAME" if $SERVERNAME;
    push @cmd, "-id=$USER"               if $USER;
    push @cmd, "-password=$PASS"         if defined $PASS && $PASS ne "";
    push @cmd, $tsm_cmd if defined $tsm_cmd;

    return @cmd;
}

# Run dsmadmc and capture output
sub run_dsmadmc {
    my ($tsm_cmd) = @_;
    my @cmd = build_dsmadmc_cmd($tsm_cmd);
    my $out = qx(@cmd 2>&1);
    my $rc  = $? >> 8;
    return ($rc,$out);
}

# Query filespaces for one node
sub fs_list_for_node {
    my ($node) = @_;
    my ($rc,$out) = run_dsmadmc("q filespace $node");
    return () if $rc != 0;

    my @names;
    my $csv = Text::CSV_XS->new({ binary => 1 });

    for my $line (split /\r?\n/, $out) {
        next unless length $line;
        if ($csv->parse($line)) {
            my @f = $csv->fields;
            if (@f >= 2 && defined $f[1] && $f[1] ne '') {
                push @names, $f[1];
            }
        }
    }
    return @names;
}

# Ensure cache is loaded for a node
sub ensure_cache_for_node {
    my ($node) = @_;
    return if exists $FS_CACHE{$node};
    my %set = map { $_ => 1 } fs_list_for_node($node);
    $FS_CACHE{$node} = \%set;
}

# Disk-space check
sub precheck_disk_space {
    my ($input_csv) = @_;
    my $input_size = -s $input_csv;
    my $need_bytes = int($input_size * 2.2);

    my $os = `uname -s`; chomp($os);
    my $df = `df -Pk . 2>/dev/null`;
    my @lines = split /\n/, $df;

    if (@lines < 2) {
        print STDERR "WARN : Could not parse 'df -Pk'; skipping free-space check.\n";
        return;
    }

    my @fields = split /\s+/, $lines[-1];
    my $avail_kb = ($os eq 'AIX') ? $fields[2] : $fields[3];
    $avail_kb = 0 unless $avail_kb =~ /^\d+$/;

    my $avail_bytes = $avail_kb * 1024;

    if ($avail_bytes < $need_bytes) {
        die sprintf("ERROR: Not enough free space. Need >= %s, available %s.\n",
            human_bytes($need_bytes), human_bytes($avail_bytes));
    }

    printf STDERR "INFO : Disk space OK (need ~%s, available %s).\n",
        human_bytes($need_bytes), human_bytes($avail_bytes);
}

# Report helpers
sub report_add_row {
    my (%row) = @_;
    push @ACTIONS, { %row };
}
sub report_flush {
    open my $fh, ">:encoding(UTF-8)", $REPORT_PATH
        or die "ERROR: Cannot write $REPORT_PATH: $!\n";
    print $fh "node,old_fs,new_fs,action,status,message\n";
    for my $r (@ACTIONS) {
        print $fh join(",", map { csv_escape($_) }
            ($r->{node},$r->{old_fs},$r->{new_fs},$r->{action},$r->{status},$r->{message})),"\n";
    }
    close $fh;
}

# Rollback helpers
sub rollback_add {
    my ($s_i,$s_a,$d_i,$d_a,$node) = @_;
    push @ROLLBACK_ROWS, [ $s_i,$s_a,$d_i,$d_a,$node ];
}
sub rollback_flush {
    open my $fh, ">:encoding(UTF-8)", $ROLLBACK_CSV
        or die "ERROR: Cannot write $ROLLBACK_CSV: $!\n";
    print $fh "OD_INSTAME_SRC,AGID_NAME_SRC,OD_INSTAME_DST,AGID_NAME_DST,OD_TSM_LOGON_NAME\n";
    for my $r (@ROLLBACK_ROWS) {
        print $fh join(",", map { csv_escape($_) } @$r),"\n";
    }
    close $fh;
}

# Password handling
sub resolve_password {
    if ($PWFILE && !$PASS) {
        open my $pf, "<:encoding(UTF-8)", $PWFILE
            or die "ERROR: Cannot read --pwfile $PWFILE: $!\n";
        chomp($PASS = <$pf> // "");
        close $pf;
    }
    if ($DO_RUN && $USER && !$PASS) {
        print STDERR "Enter password for '$USER': ";
        system("stty -echo");
        chomp($PASS = <STDIN> // "");
        system("stty echo");
        print STDERR "\n";
    }
}

# Macro helpers
sub batch_queue {
    my ($n,$o,$w) = @_;
    push @MACRO_LINES, "rename filespace $n ".quote_tsm($o)." ".quote_tsm($w);
}
sub batch_execute {
    return (0,"") unless @MACRO_LINES;

    open my $mf, ">:encoding(UTF-8)", $MACRO_PATH
        or die "ERROR: Cannot write $MACRO_PATH: $!\n";
    print $mf "$_\n" for @MACRO_LINES;
    print $mf "quit\n";
    close $mf;

    my $abs = abs_path($MACRO_PATH) // $MACRO_PATH;
    print STDERR "INFO : Executing macro ($abs) with ".scalar(@MACRO_LINES)." rename commands\n";

    my @cmd = build_dsmadmc_cmd("macro $abs");
    my $out = qx(@cmd 2>&1);
    my $rc  = $? >> 8;

    print STDERR "INFO : Macro rc=$rc\n";
    unlink $abs unless $KEEP_BATCH;
    return ($rc,$out);
}

# Verify single pair (after macro)
sub verify_pair_once {
    my ($node,$old_fs,$new_fs) = @_;
    my %set = map { $_ => 1 } fs_list_for_node($node);

    if ($set{$new_fs} && !$set{$old_fs}) {
        return ("OK","verified");
    }
    if ($set{$new_fs} && $set{$old_fs}) {
        return ("WARN","both old and new present");
    }
    return ("OK","executed (not re-verified)");
}

# ------------------------------- Main --------------------------------------

precheck_disk_space($CSV_FILE);
resolve_password();

# Parse input CSV
my $csv = Text::CSV_XS->new({ binary => 1, sep_char => $DELIM, auto_diag => 1 });
open my $inf, "<:encoding(UTF-8)", $CSV_FILE
    or die "ERROR: Cannot read $CSV_FILE: $!\n";
my $hdr = $csv->getline($inf) or die "ERROR: Empty CSV or cannot read header\n";
$csv->column_names(@$hdr);

# Validate headers
my %need = map { $_ => 1 } qw/OD_INSTAME_SRC AGID_NAME_SRC OD_INSTAME_DST AGID_NAME_DST OD_TSM_LOGON_NAME/;
for my $h (@$hdr) { delete $need{$h} if exists $need{$h} }
die "ERROR: CSV missing required headers: ".join(", ", sort keys %need)."\n" if %need;

while (my $row = $csv->getline_hr($inf)) {
    my $node     = clean_scalar($row->{OD_TSM_LOGON_NAME});
    my $src_inst = clean_scalar($row->{OD_INSTAME_SRC});
    my $src_agid = clean_scalar($row->{AGID_NAME_SRC});
    my $dst_inst = clean_scalar($row->{OD_INSTAME_DST});
    my $dst_agid = clean_scalar($row->{AGID_NAME_DST});

    if ($node eq "") {
        report_add_row(node=>"", old_fs=>"", new_fs=>"", action=>"skip", status=>"SKIPPED", message=>"OD_TSM_LOGON_NAME empty");
        next;
    }

    my ($old_inst,$old_agid,$new_inst,$new_agid) = $UNDO
        ? ($dst_inst,$dst_agid,$src_inst,$src_agid)
        : ($src_inst,$src_agid,$dst_inst,$dst_agid);

    my $old_fs = "/$old_inst/$old_agid";
    my $new_fs = "/$new_inst/$new_agid";

    ensure_cache_for_node($node);

    # Undo-specific checks
    if ($UNDO) {
        my $has_old = $FS_CACHE{$node}{$old_fs} ? 1 : 0;
        my $has_new = $FS_CACHE{$node}{$new_fs} ? 1 : 0;
        if ($has_new && !$has_old) {
            report_add_row(node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename", status=>"OK", message=>"already rolled back");
            next;
        }
        if ($has_new && $has_old) {
            report_add_row(node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename", status=>"WARN", message=>"both old and new present; manual check");
            next;
        }
    }

    # Standard checks
    unless ($FS_CACHE{$node}{$old_fs}) {
        report_add_row(node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename", status=>"SKIPPED", message=>"old_fs not found");
        next;
    }
    if ($FS_CACHE{$node}{$new_fs}) {
        report_add_row(node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename", status=>"SKIPPED", message=>"new_fs already exists");
        next;
    }

    unless ($DO_RUN) {
        my $cmd = "rename filespace $node ".quote_tsm($old_fs)." ".quote_tsm($new_fs);
        print STDERR "INFO : [DRY] $cmd\n";
        report_add_row(node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename", status=>"DRY-RUN", message=>"No changes applied");
        next;
    }

    # RUN
    batch_queue($node,$old_fs,$new_fs);
    delete $FS_CACHE{$node}{$old_fs};
    $FS_CACHE{$node}{$new_fs} = 1;

    report_add_row(node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename", status=>"PENDING", message=>"queued in macro");

    my ($rb_src_i,$rb_src_a,$rb_dst_i,$rb_dst_a) = $UNDO
        ? ($src_inst,$src_agid,$dst_inst,$dst_agid)
        : ($dst_inst,$dst_agid,$src_inst,$src_agid);
    rollback_add($rb_src_i,$rb_src_a,$rb_dst_i,$rb_dst_a,$node);

    $VERIFY_NODES{$node}++ if $VERIFY;
}
close $inf;

# Execute macro
if ($DO_RUN) {
    my ($rc,$out) = batch_execute();

    if (!$VERIFY) {
        for my $r (@ACTIONS) {
            next unless $r->{status} eq "PENDING";
            $r->{status}  = "OK";
            $r->{message} = "submitted via macro (not verified)";
        }
    }

    if ($VERIFY) {
        for my $node (keys %VERIFY_NODES) {
            for my $r (@ACTIONS) {
                next unless $r->{node} eq $node && $r->{status} eq "PENDING";
                my ($st,$why) = verify_pair_once($node, $r->{old_fs}, $r->{new_fs});
                $r->{status}  = $st;
                $r->{message} = "renamed; $why";
            }
        }
    }
}

# Write outputs & summary
report_flush();
rollback_flush();

my %stats; $stats{ $_->{status} }++ for @ACTIONS;
printf STDERR "INFO : Done. Report: %s | Rollback CSV: %s\n", $REPORT_PATH, $ROLLBACK_CSV;
printf STDERR "INFO : Summary -> OK:%d WARN:%d FAILED:%d SKIPPED:%d DRY-RUN:%d PENDING:%d\n",
    ($stats{OK}||0),($stats{WARN}||0),($stats{FAILED}||0),($stats{SKIPPED}||0),($stats{'DRY-RUN'}||0),($stats{PENDING}||0);

# -------------------------------- Help -------------------------------------
# -------------------------------- Help -------------------------------------

sub print_usage {
    print <<'USAGE';
Usage:
  cmod_fs_rename.pl -c <file.csv>
                    [--servername STANZA]
                    [-u USER]
                    [-p PASS | --pwfile FILE]
                    [--delim ';']
                    [--run]
                    [--verify]
                    [--undo]
                    [--keep-batch]

Description:
  Batch-only tool to rename Spectrum Protect filespaces based on a CSV mapping,
  so CMOD payloads remain correctly linked after federation.

Direction:
  default: old_fs=/OD_INSTAME_SRC/AGID_NAME_SRC  -> new_fs=/OD_INSTAME_DST/AGID_NAME_DST
  --undo : old_fs=/OD_INSTAME_DST/AGID_NAME_DST  -> new_fs=/OD_INSTAME_SRC/AGID_NAME_SRC
           - If new_fs exists and old_fs is missing → OK "already rolled back"
           - If both exist → WARN "both old and new present" (no rename queued)

Verification:
  --verify : After the macro is executed, verify each affected node once using
             'q filespace <node>'. Rows become:
               - OK   : new_fs present and old_fs absent
               - WARN : both present
               - OK   : executed (not re-verified) — macro rc==0, but snapshot
                        still shows old state

Outputs:
  - Report CSV   : fs_rename_report_<timestamp>.csv
  - Rollback CSV : fs_rollback_csv_<timestamp>.csv (inverse mappings)

Notes:
  - Default is DRY-RUN (no changes) until --run is used.
  - dsmadmc is invoked with: -dataonly=yes -comma -noconfirm
  - Required CSV headers (exact):
        OD_INSTAME_SRC, AGID_NAME_SRC, OD_INSTAME_DST, AGID_NAME_DST, OD_TSM_LOGON_NAME
  - Script checks free space >= 2.2 × input CSV size (report + rollback + slack).

Examples:
  # Dry-run (no changes), semicolon delimiter:
  ./cmod_fs_rename.pl -c mapping.csv --delim ';'

  # Apply changes (batch macro), verify after macro:
  ./cmod_fs_rename.pl -c mapping.csv --servername DRARD11B -u ondemand -p 'secret' --run --verify

  # Generate rollback CSV for the above (undo direction):
  ./cmod_fs_rename.pl -c mapping.csv --servername DRARD11B -u ondemand -p 'secret' --run --undo --verify

  # Keep the generated macro file for audit:
  ./cmod_fs_rename.pl -c mapping.csv --run --keep-batch
USAGE
}