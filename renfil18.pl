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
# Version:    1.6 (batch-only, readable)
#
# High-level flow:
#   1) Read input CSV (required headers listed below).
#   2) For each row build old_fs=/SRC/AGID and new_fs=/DST/AGID (or reversed with --undo).
#   3) Per node, load filespace cache via single 'q filespace <node>' (once per node).
#      - Skip when old_fs missing or new_fs already exists (report SKIPPED).
#      - If --run: queue 'rename filespace ...' into one macro (optimistically update cache).
#      - If no --run: DRY-RUN (report only, no macro).
#      - Record rollback (inverse mapping) for queued renames.
#   4) If --run: write the macro and execute it once (one dsmadmc login).
#   5) Verification:
#        --verify       : 1× 'q filespace <node>' per affected node after macro; update statuses.
#        --verify-late  : to the same effect, but performed after macro as a distinct phase.
#        (If neither verify is requested, queued PENDING rows become OK: "submitted via macro".)
#   6) Write report CSV + rollback CSV, print summary.
#
# Default = DRY-RUN (no changes). Add --run to apply changes.
#
# dsmadmc flags used everywhere:  -dataonly=yes  -comma  -noconfirm
#
# Required CSV headers (exact names):
#   OD_INSTAME_SRC, AGID_NAME_SRC, OD_INSTAME_DST, AGID_NAME_DST, OD_TSM_LOGON_NAME
#
# Usage:
#   cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER]
#                     [-p PASS | --pwfile FILE] [--delim ';']
#                     [--run] [--verify | --verify-late] [--undo] [--keep-batch]
##############################################################################

use strict;
use warnings;
use POSIX qw(strftime);
use Text::CSV_XS;
use Cwd qw(abs_path);

# ------------------------------ CLI & Globals ------------------------------

my ($CSV_FILE,$SERVERNAME,$USER,$PASS,$PWFILE,$DELIM) = ("","","","","",",");
my ($DO_RUN,$VERIFY,$VERIFY_LATE,$UNDO,$KEEP_BATCH) = (0,0,0,0,0);

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
    elsif ($arg eq '--keep-batch')                 { $KEEP_BATCH = 1 }
    elsif ($arg eq '-h' or $arg eq '--help')       { print_usage(); exit 0 }
    else { die "Unknown arg: $arg (use --help)\n" }
}

die "ERROR: CSV file required (-c|--csv)\n" unless $CSV_FILE;
-f $CSV_FILE or die "ERROR: CSV not readable: $CSV_FILE\n";

# Stamp & output files
my $STAMP        = strftime("%Y%m%d_%H%M%S", localtime);
my $REPORT_PATH  = "fs_rename_report_${STAMP}.csv";
my $ROLLBACK_CSV = "fs_rollback_csv_${STAMP}.csv";
my $MACRO_PATH   = "fs_batch_${STAMP}.mac";

# In-memory state
my @ACTIONS;        # Report rows (hashrefs)
my @ROLLBACK_ROWS;  # Rollback rows (arrayrefs)
my %FS_CACHE;       # FS_CACHE{node}{filespace}=1 per node, loaded once
my %VERIFY_NODES;   # nodes to verify at the end (verify or verify-late)
my @MACRO_LINES;    # collected 'rename filespace ...' commands

# ------------------------------ Utilities ----------------------------------

sub quote_tsm {
    my ($s) = @_; $s //= ""; $s =~ s/"/""/g; return qq{"$s"};
}
sub human_bytes {
    my ($b) = @_; $b ||= 0; my @u=('B','KB','MB','GB','TB'); my $i=0;
    while ($b>=1024 && $i<$#u){ $b/=1024; $i++ }
    return $i==0 ? sprintf('%d %s',$b,$u[$i]) : sprintf('%.1f %s',$b,$u[$i]);
}
sub clean_scalar {
    my ($v)=@_; $v//= ""; $v =~ s/\r$//; $v =~ s/^\s+|\s+$//g; return $v;
}
sub csv_escape {
    my ($s)=@_; $s//= ""; if ($s =~ /[",\r\n]/){ $s =~ s/"/""/g; return qq{"$s"} } return $s;
}

sub build_dsmadmc_cmd {
    my ($tsm_cmd)=@_;
    my @cmd=("dsmadmc","-dataonly=yes","-comma","-noconfirm");
    push @cmd, "-servername=$SERVERNAME" if $SERVERNAME;
    push @cmd, "-id=$USER"               if $USER;
    push @cmd, "-password=$PASS"         if defined $PASS && $PASS ne "";
    push @cmd, $tsm_cmd if defined $tsm_cmd;
    return @cmd;
}
sub run_dsmadmc {
    my ($tsm_cmd)=@_;
    my @cmd=build_dsmadmc_cmd($tsm_cmd);
    my $out=qx(@cmd 2>&1); my $rc=$?>>8;
    return ($rc,$out);
}

# Query filespaces for a node (once per node)
sub fs_list_for_node {
    my ($node)=@_;
    my ($rc,$out)=run_dsmadmc("q filespace $node");
    return () if $rc!=0;
    my @names; my $csv = Text::CSV_XS->new({ binary=>1 });
    for my $line (split /\r?\n/,$out){
        next unless length $line;
        if ($csv->parse($line)) {
            my @f=$csv->fields;
            push @names,$f[1] if @f>=2 && defined $f[1] && $f[1] ne '';
        }
    }
    return @names;
}
sub ensure_cache_for_node {
    my ($node)=@_;
    return if exists $FS_CACHE{$node};
    my %set = map { $_=>1 } fs_list_for_node($node);
    $FS_CACHE{$node}=\%set;
}

# Free-space precheck: needs ≥ 2.2 × input CSV size (report+rollback+slack)
sub precheck_disk_space {
    my ($input_csv)=@_;
    my $input_size=-s $input_csv; my $need_bytes=int($input_size*2.2);
    my $out_dir="."; my $os=`uname -s`; chomp($os);
    my $df=`df -Pk $out_dir 2>/dev/null`; my @lines=split /\n/,$df;
    if(@lines<2){ print STDERR "WARN : Could not parse 'df -Pk'; skipping free-space check.\n"; return; }
    my @f=split /\s+/,$lines[-1]; my $avail_kb=($os eq 'AIX')? $f[2]:$f[3];
    $avail_kb=0 unless defined $avail_kb && $avail_kb=~/^\d+$/;
    my $avail_bytes=$avail_kb*1024;
    if($avail_bytes<$need_bytes){
        die sprintf("ERROR: Not enough free space. Need >= %s, available %s.\n",
                    human_bytes($need_bytes), human_bytes($avail_bytes));
    }
    printf STDERR "INFO : Disk space OK (need ~%s, available %s).\n",
                  human_bytes($need_bytes), human_bytes($avail_bytes);
}

# Report & rollback helpers (buffered, flushed at end)
sub report_add_row {
    my (%row)=@_; push @ACTIONS, { %row };
}
sub report_flush_to_file {
    open my $rfh, ">:encoding(UTF-8)", $REPORT_PATH or die "ERROR: Cannot write $REPORT_PATH: $!\n";
    print $rfh "node,old_fs,new_fs,action,status,message\n";
    for my $r (@ACTIONS){
        print $rfh join(",", map { csv_escape($_) } ($r->{node},$r->{old_fs},$r->{new_fs},$r->{action},$r->{status},$r->{message})), "\n";
    }
    close $rfh;
}
sub rollback_add_row {
    my ($src_inst,$src_agid,$dst_inst,$dst_agid,$node)=@_;
    push @ROLLBACK_ROWS, [ $src_inst,$src_agid,$dst_inst,$dst_agid,$node ];
}
sub rollback_flush_to_file {
    open my $rbfh, ">:encoding(UTF-8)", $ROLLBACK_CSV or die "ERROR: Cannot write $ROLLBACK_CSV: $!\n";
    print $rbfh "OD_INSTAME_SRC,AGID_NAME_SRC,OD_INSTAME_DST,AGID_NAME_DST,OD_TSM_LOGON_NAME\n";
    for my $r (@ROLLBACK_ROWS){
        print $rbfh join(",", map { csv_escape($_) } @$r), "\n";
    }
    close $rbfh;
}

# Password handling
sub resolve_password {
    if ($PWFILE && !$PASS) {
        open my $pfh, "<:encoding(UTF-8)", $PWFILE or die "ERROR: Cannot read --pwfile $PWFILE: $!\n";
        chomp($PASS = <$pfh> // ""); close $pfh;
    }
    if ($DO_RUN && $USER && !$PASS) {
        print STDERR "Enter password for '$USER': ";
        system("stty -echo"); chomp($PASS = <STDIN> // ""); system("stty echo"); print STDERR "\n";
    }
}

# Batch helpers
sub batch_queue_rename {
    my ($node,$old_fs,$new_fs)=@_;
    my $cmd = "rename filespace $node ".quote_tsm($old_fs)." ".quote_tsm($new_fs);
    push @MACRO_LINES, $cmd;
}
sub batch_run_macro_once {
    return (0,"") unless @MACRO_LINES;   # nothing to do
    open my $mf, ">:encoding(UTF-8)", $MACRO_PATH or die "ERROR: Cannot write $MACRO_PATH: $!\n";
    print $mf "$_\n" for @MACRO_LINES; print $mf "quit\n"; close $mf;
    my $abs = abs_path($MACRO_PATH) // $MACRO_PATH;
    print STDERR "INFO : Executing macro ($abs) with ".scalar(@MACRO_LINES)." rename commands\n";
    my @cmd = build_dsmadmc_cmd("macro $abs");
    my $out = qx(@cmd 2>&1); my $rc = $? >> 8;
    print STDERR "INFO : Macro rc=$rc\n";
    unlink $abs unless $KEEP_BATCH;
    return ($rc,$out);
}
sub verify_node_pair {
    my ($node,$old_fs,$new_fs)=@_;
    my %set = map { $_=>1 } fs_list_for_node($node);
    if ($set{$new_fs} && !$set{$old_fs}) { return ("OK","verified") }
    elsif ($set{$new_fs} && $set{$old_fs}) { return ("WARN","both old and new present") }
    else { return ("WARN","verification mismatch") }
}

# ------------------------------ Main Flow ----------------------------------

# 0) Free-space check
precheck_disk_space($CSV_FILE);

# 1) Resolve password if needed
resolve_password();

# 2) Prepare CSV reader, enforce headers
my $csv_in = Text::CSV_XS->new({ binary=>1, sep_char=>$DELIM, auto_diag=>1 });
open my $fh, "<:encoding(UTF-8)", $CSV_FILE or die "ERROR: Cannot read $CSV_FILE: $!\n";
my $header = $csv_in->getline($fh) or die "ERROR: Empty CSV or cannot read header\n";
$csv_in->column_names(@$header);

my %idx; for my $i(0..$#$header){ $idx{$header->[$i]}=$i; }
for my $need (qw/OD_INSTAME_SRC AGID_NAME_SRC OD_INSTAME_DST AGID_NAME_DST OD_TSM_LOGON_NAME/){
    die "ERROR: CSV missing required header: $need\n" unless exists $idx{$need};
}

# 3) Read and plan actions
while (my $row = $csv_in->getline_hr($fh)) {

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
        ? ($dst_inst,$dst_agid,$src_inst,$src_agid)  # undo: DST->SRC
        : ($src_inst,$src_agid,$dst_inst,$dst_agid); # normal: SRC->DST

    my $old_fs = "/$old_inst/$old_agid";
    my $new_fs = "/$new_inst/$new_agid";

    # Load per-node cache once
    ensure_cache_for_node($node);

    # Pre-checks
    unless ($FS_CACHE{$node}{$old_fs}) {
        report_add_row(node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename", status=>"SKIPPED", message=>"old_fs not found");
        next;
    }
    if ($FS_CACHE{$node}{$new_fs}) {
        report_add_row(node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename", status=>"SKIPPED", message=>"new_fs already exists");
        next;
    }

    if (!$DO_RUN) {
        # DRY-RUN
        my $cmd = "rename filespace $node ".quote_tsm($old_fs)." ".quote_tsm($new_fs);
        print STDERR "INFO : [DRY] $cmd\n";
        report_add_row(node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename", status=>"DRY-RUN", message=>"No changes applied");
        next;
    }

    # RUN: queue into macro and update cache optimistically
    batch_queue_rename($node,$old_fs,$new_fs);
    delete $FS_CACHE{$node}{$old_fs}; $FS_CACHE{$node}{$new_fs}=1;

    report_add_row(node=>$node, old_fs=>$old_fs, new_fs=>$new_fs, action=>"rename", status=>"PENDING", message=>"queued in macro");

    # Rollback CSV row = inverse mapping of intended action
    my ($rb_src_inst,$rb_src_agid,$rb_dst_inst,$rb_dst_agid) = $UNDO
        ? ($src_inst,$src_agid,$dst_inst,$dst_agid)
        : ($dst_inst,$dst_agid,$src_inst,$src_agid);
    rollback_add_row($rb_src_inst,$rb_src_agid,$rb_dst_inst,$rb_dst_agid,$node);

    # Mark node for verification if any verify mode is used later
    $VERIFY_NODES{$node}++ if ($VERIFY || $VERIFY_LATE);
}
close $fh;

# 4) Execute macro once (if run mode)
if ($DO_RUN) {
    my ($mrc,$mout) = batch_run_macro_once();

    # If neither verify mode requested, mark PENDING -> OK (submitted)
    if (!$VERIFY && !$VERIFY_LATE) {
        for my $r (@ACTIONS) {
            next unless $r->{status} eq "PENDING";
            $r->{status}  = "OK";
            $r->{message} = "submitted via macro (not verified)";
        }
    }

    # If --verify (standard batch verify): per-node snapshot & update PENDING rows
    if ($VERIFY && !$VERIFY_LATE) {
        for my $node (keys %VERIFY_NODES) {
            my %set = map { $_=>1 } fs_list_for_node($node);
            for my $r (@ACTIONS) {
                next unless $r->{node} eq $node && $r->{status} eq "PENDING";
                my ($old,$new)=($r->{old_fs},$r->{new_fs});
                if ($set{$new} && !$set{$old}) {
                    $r->{status}  = "OK";
                    $r->{message} = "renamed; verified";
                } elsif ($set{$new} && $set{$old}) {
                    $r->{status}  = "WARN";
                    $r->{message} = "both old and new present";
                } else {
                    $r->{status}  = "WARN";
                    $r->{message} = "verification mismatch";
                }
            }
        }
    }
}

# 5) Late verification (batch-only as well): do it after macro for all nodes
if ($DO_RUN && $VERIFY_LATE) {
    for my $node (keys %VERIFY_NODES) {
        my %set = map { $_=>1 } fs_list_for_node($node);
        for my $r (@ACTIONS) {
            next unless $r->{node} eq $node && ($r->{status} eq "PENDING" || $r->{message} =~ /submitted via macro/);
            my ($old,$new)=($r->{old_fs},$r->{new_fs});
            if ($set{$new} && !$set{$old}) {
                $r->{status}  = "OK";
                $r->{message} = "renamed; verified (late)";
            } elsif ($set{$new} && $set{$old}) {
                $r->{status}  = "WARN";
                $r->{message} = "both old and new present (late verify)";
            } else {
                $r->{status}  = "WARN";
                $r->{message} = "late verify mismatch";
            }
        }
    }
}

# 6) Flush outputs and print summary
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
                    [--delim ';'] [--run] [--verify | --verify-late] [--undo] [--keep-batch]

Direction:
  default: old_fs=/OD_INSTAME_SRC/AGID_NAME_SRC  -> new_fs=/OD_INSTAME_DST/AGID_NAME_DST
  --undo : old_fs=/OD_INSTAME_DST/AGID_NAME_DST  -> new_fs=/OD_INSTAME_SRC/AGID_NAME_SRC

Mode:
  - Batch-only: all valid renames are queued and executed in a single dsmadmc macro.
    (One login, faster, stable.)

Verification:
  --verify       : after macro, query each affected node once and set OK/WARN.
  --verify-late  : same idea, but performed as a final pass (useful when you prefer
                   to decouple execution and verification timing).
  (If neither is set, queued rows become OK with message "submitted via macro".)

Outputs:
  - Report CSV   : fs_rename_report_<timestamp>.csv
  - Rollback CSV : fs_rollback_csv_<timestamp>.csv  (inverse mappings)

Notes:
  - Default is DRY-RUN (no changes).
  - dsmadmc is invoked with: -dataonly=yes -comma -noconfirm
  - Required CSV headers: OD_INSTAME_SRC, AGID_NAME_SRC, OD_INSTAME_DST,
                          AGID_NAME_DST, OD_TSM_LOGON_NAME
  - Script checks free space >= 2.2 × input CSV size (report + rollback + slack).
USAGE
}