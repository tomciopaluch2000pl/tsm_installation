##############################################################################
# Script:     cmod_fs_rename.pl
# Purpose:    Rename Spectrum Protect filespaces to align payloads between
#             federated CMOD (Content Manager OnDemand) and federated
#             Spectrum Protect, ensuring proper linkage of archived payloads
#             after federation.
#
# Project:    LA HUB
# Author:     <Your Name / Team>
# Date:       2025-08-27
# Version:    1.1
#
# Usage:
#   cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER]
#                     [-p PASS | --pwfile FILE] [--delim ';']
#                     [--run] [--verify] [--undo]
#
# What it does:
#   - Reads mapping from CSV (required headers):
#       OD_INSTAME_SRC, AGID_NAME_SRC, OD_INSTAME_DST, AGID_NAME_DST, OD_TSM_LOGON_NAME
#   - Direction:
#       default: old_fs = /OD_INSTAME_SRC/AGID_NAME_SRC
#                new_fs = /OD_INSTAME_DST/AGID_NAME_DST
#       --undo : old_fs = /OD_INSTAME_DST/AGID_NAME_DST
#                new_fs = /OD_INSTAME_SRC/AGID_NAME_SRC
#   - Skips rows with empty OD_TSM_LOGON_NAME (CMOD used only CACHE) and logs that.
#   - Default is DRY-RUN (prints planned 'rename filespace', no changes).
#   - With --run:
#       * Pre-checks: old_fs must exist; new_fs must NOT exist.
#       * Executes:   rename filespace <node> "<old_fs>" "<new_fs>"
#       * --verify :  re-queries filespaces to confirm success.
#
# Outputs:
#   * Report CSV     : fs_rename_report_YYYYmmdd_HHMMSS.csv
#   * Rollback CSV   : fs_rollback_csv_YYYYmmdd_HHMMSS.csv
#                      (same header as input; values swapped so it can be fed
#                       back to this script with --run to restore previous state)
#
# dsmadmc flags:
#   -dataonly=yes -comma -noconfirm
##############################################################################

#!/usr/bin/env perl
use strict;
use warnings;
use POSIX qw(strftime);
use Text::CSV_XS;   # requires perl-Text-CSV_XS package

# ---------------- CLI ----------------
my ($CSV_FILE,$SERVERNAME,$USER,$PASS,$PWFILE,$DELIM) = ("","","","","",",");
my ($DO_RUN,$VERIFY,$UNDO) = (0,0,0);

while (my $arg = shift @ARGV) {
    if    ($arg eq '-c' or $arg eq '--csv')        { $CSV_FILE   = shift(@ARGV) // "" }
    elsif ($arg eq '--servername')                 { $SERVERNAME = shift(@ARGV) // "" }
    elsif ($arg eq '-u' or $arg eq '--user')       { $USER       = shift(@ARGV) // "" }
    elsif ($arg eq '-p' or $arg eq '--pass')       { $PASS       = shift(@ARGV) // "" }
    elsif ($arg eq '--pwfile')                     { $PWFILE     = shift(@ARGV) // "" }
    elsif ($arg eq '--delim')                      { $DELIM      = shift(@ARGV) // "," }
    elsif ($arg eq '--run')                        { $DO_RUN     = 1 }
    elsif ($arg eq '--verify')                     { $VERIFY     = 1 }
    elsif ($arg eq '--undo')                       { $UNDO       = 1 }
    elsif ($arg eq '-h' or $arg eq '--help')       { print_usage(); exit 0 }
    else { die "Unknown arg: $arg (use --help)\n" }
}

die "ERROR: CSV file required (-c|--csv)\n" unless $CSV_FILE;
-f $CSV_FILE or die "ERROR: CSV not readable: $CSV_FILE\n";

# Password resolution
if ($PWFILE && !$PASS) {
    open my $pfh, "<:encoding(UTF-8)", $PWFILE or die "ERROR: Cannot read --pwfile $PWFILE: $!\n";
    chomp($PASS = <$pfh> // "");
    close $pfh;
}
if ($DO_RUN && $USER && !$PASS) {
    print STDERR "Enter password for '$USER': ";
    system("stty -echo"); chomp($PASS = <STDIN> // ""); system("stty echo"); print STDERR "\n";
}

# ---------------- Helpers ----------------
sub nowstamp { return strftime("%Y%m%d_%H%M%S", localtime); }

# Quote value for dsmadmc (double quotes, inner quotes doubled)
sub q_tsm {
    my ($s)=@_; $s //= "";
    $s =~ s/"/""/g; return qq{"$s"};
}

# Build dsmadmc cmd with stable flags; -noconfirm to suppress Y/N prompts
sub build_dsmadmc_cmd {
    my ($tsm_cmd) = @_;
    my @cmd = ("dsmadmc", "-dataonly=yes", "-comma", "-noconfirm");
    push @cmd, "-servername=$SERVERNAME" if $SERVERNAME;
    push @cmd, "-id=$USER"               if $USER;
    push @cmd, "-password=$PASS"         if defined $PASS && $PASS ne "";
    push @cmd, $tsm_cmd;
    return @cmd;
}

# Portable runner: capture stdout+stderr together; return (rc, out, "")
sub run_dsmadmc {
    my ($tsm_cmd) = @_;
    my @cmd = build_dsmadmc_cmd($tsm_cmd);
    my $out = qx(@cmd 2>&1);
    my $rc  = $? >> 8;
    return ($rc, $out, "");
}

# Query filespaces for node; with -comma output the 2nd column is FILESPACE_NAME
sub fs_list_for_node {
    my ($node) = @_;
    my ($rc, $out, $err) = run_dsmadmc("q filespace $node");
    return () if $rc != 0;
    my @names;
    my $csv = Text::CSV_XS->new({ binary=>1 });
    for my $line (split /\r?\n/, $out) {
        next unless length $line;
        if ($csv->parse($line)) {
            my @f = $csv->fields;
            next unless @f >= 2;
            push @names, $f[1];
        }
    }
    return @names;
}
sub fs_exists {
    my ($node,$fs)=@_;
    for my $n (fs_list_for_node($node)) { return 1 if defined $n && $n eq $fs }
    return 0;
}

# Field cleanup: strip trailing CR (Windows CRLF) and trim spaces
sub clean {
    my ($v)=@_; $v //= "";
    $v =~ s/\r$//; $v =~ s/^\s+|\s+$//g; return $v;
}

# Minimal CSV escaping for the report/rollback files
sub csv_escape {
    my ($s)=@_; $s //= "";
    if ($s =~ /[",\r\n]/) { $s =~ s/"/""/g; return qq{"$s"}; }
    return $s;
}

# ---------------- Outputs ----------------
my $stamp         = nowstamp();
my $REPORT_PATH   = "fs_rename_report_${stamp}.csv";
my $ROLLBACK_CSV  = "fs_rollback_csv_${stamp}.csv";   # <— rollback as CSV

open my $rep, ">:encoding(UTF-8)", $REPORT_PATH or die "ERROR: Cannot write $REPORT_PATH: $!\n";
print $rep "node,old_fs,new_fs,action,status,message\n";
close $rep;

# Rollback CSV has the SAME header as input, so it can be fed back to this script.
open my $rbc, ">:encoding(UTF-8)", $ROLLBACK_CSV or die "ERROR: Cannot write $ROLLBACK_CSV: $!\n";
print $rbc "OD_INSTAME_SRC,AGID_NAME_SRC,OD_INSTAME_DST,AGID_NAME_DST,OD_TSM_LOGON_NAME\n";
close $rbc;

sub append_report {
    my ($node,$old,$new,$action,$status,$msg) = @_;
    open my $rfh, ">>:encoding(UTF-8)", $REPORT_PATH or die "Cannot append $REPORT_PATH: $!";
    print $rfh join(",", map { csv_escape($_) } ($node,$old,$new,$action,$status,$msg)), "\n";
    close $rfh;
}
sub append_rollback_csv_row {
    my ($src_inst,$src_agid,$dst_inst,$dst_agid,$node) = @_;
    open my $rbfh, ">>:encoding(UTF-8)", $ROLLBACK_CSV or die "Cannot append $ROLLBACK_CSV: $!";
    print $rbfh join(",", map { csv_escape($_) }
        ($src_inst,$src_agid,$dst_inst,$dst_agid,$node)), "\n";
    close $rbfh;
}

# ---------------- CSV input ----------------
my $csv = Text::CSV_XS->new({ binary => 1, sep_char => $DELIM, auto_diag => 1 });
open my $fh, "<:encoding(UTF-8)", $CSV_FILE or die "ERROR: Cannot read $CSV_FILE: $!\n";

# Read header and enforce required names
my $header = $csv->getline($fh) or die "ERROR: Empty CSV or cannot read header\n";
$csv->column_names(@$header);
my %idx; for my $i (0..$#$header){ $idx{$header->[$i]} = $i; }
for my $need (qw/OD_INSTAME_SRC AGID_NAME_SRC OD_INSTAME_DST AGID_NAME_DST OD_TSM_LOGON_NAME/) {
    die "ERROR: CSV missing required header: $need\n" unless exists $idx{$need};
}

# ---------------- Main loop ----------------
while (my $row = $csv->getline_hr($fh)) {
    my $node     = clean($row->{OD_TSM_LOGON_NAME});
    if ($node eq "") {
        append_report("", "", "", "skip", "SKIPPED", "OD_TSM_LOGON_NAME empty");
        next;
    }

    my $src_inst_in = clean($row->{OD_INSTAME_SRC});
    my $src_agid_in = clean($row->{AGID_NAME_SRC});
    my $dst_inst_in = clean($row->{OD_INSTAME_DST});
    my $dst_agid_in = clean($row->{AGID_NAME_DST});

    # Direction control (normal vs undo)
    my ($old_inst, $old_agid, $new_inst, $new_agid) = $UNDO
        ? ($dst_inst_in, $dst_agid_in, $src_inst_in, $src_agid_in)   # undo: reverse
        : ($src_inst_in, $src_agid_in, $dst_inst_in, $dst_agid_in);  # normal

    my $old_fs = "/$old_inst/$old_agid";
    my $new_fs = "/$new_inst/$new_agid";
    my $tsm_cmd = "rename filespace $node " . q_tsm($old_fs) . " " . q_tsm($new_fs);

    # DRY-RUN
    if (!$DO_RUN) {
        print STDERR "INFO : [DRY] $tsm_cmd\n";
        append_report($node, $old_fs, $new_fs, "rename", "DRY-RUN", "No changes applied");
        next;
    }

    # Pre-checks
    unless (fs_exists($node, $old_fs)) {
        append_report($node, $old_fs, $new_fs, "rename", "SKIPPED", "old_fs not found");
        next;
    }
    if (fs_exists($node, $new_fs)) {
        append_report($node, $old_fs, $new_fs, "rename", "SKIPPED", "new_fs already exists");
        next;
    }

    # Execute
    print STDERR "INFO : Executing: $tsm_cmd\n";
    my ($rc, $out, $err) = run_dsmadmc($tsm_cmd);
    if ($rc != 0) {
        my $msg = $out || "";
        $msg =~ s/[\r\n,]/ /g;
        append_report($node, $old_fs, $new_fs, "rename", "FAILED", $msg);
        next;
    }

    my $status = "OK";
    my $msg    = "renamed";

    # Verify (optional)
    if ($VERIFY) {
        my $has_new = fs_exists($node, $new_fs);
        my $has_old = fs_exists($node, $old_fs);
        if ($has_new && !$has_old) { $msg .= "; verified"; }
        else { $status = "WARN"; $msg .= "; verification mismatch"; }
    }

    append_report($node, $old_fs, $new_fs, "rename", $status, $msg);

    # Rollback CSV row:
    #   We always emit the inverse mapping of what we JUST applied.
    #   So feeding this CSV back (with --run) restores the original state.
    my ($rb_src_inst,$rb_src_agid,$rb_dst_inst,$rb_dst_agid) = ($new_inst,$new_agid,$old_inst,$old_agid);
    append_rollback_csv_row($rb_src_inst,$rb_src_agid,$rb_dst_inst,$rb_dst_agid,$node);
}
close $fh;

print STDERR "INFO : Done. Report: $REPORT_PATH\n";
print STDERR "INFO : Rollback CSV: $ROLLBACK_CSV\n";

# ---------------- Usage/help ----------------
sub print_usage {
    print <<'USAGE';
Usage:
  cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER] [-p PASS | --pwfile FILE]
                    [--delim ';'] [--run] [--verify] [--undo]

Direction:
  default: old_fs=/OD_INSTAME_SRC/AGID_NAME_SRC  -> new_fs=/OD_INSTAME_DST/AGID_NAME_DST
  --undo : old_fs=/OD_INSTAME_DST/AGID_NAME_DST  -> new_fs=/OD_INSTAME_SRC/AGID_NAME_SRC

Rollback options:
  1) During --run the script writes fs_rollback_csv_*.csv with the inverse mapping.
     To rollback later, run:
       cmod_fs_rename.pl -c fs_rollback_csv_*.csv --servername STANZA --run [--verify]
  2) Alternatively, you can use the original CSV with --undo:
       cmod_fs_rename.pl -c original.csv --servername STANZA --run --undo [--verify]

Notes:
  - Default is DRY-RUN (no changes).
  - dsmadmc is invoked with: -dataonly=yes -comma -noconfirm
  - Required CSV headers (exact names):
      OD_INSTAME_SRC, AGID_NAME_SRC, OD_INSTAME_DST, AGID_NAME_DST, OD_TSM_LOGON_NAME
USAGE
}