#!/usr/bin/env perl
##############################################################################
# Script:     cmod_fs_rename.pl
# Purpose:    Rename Spectrum Protect filespaces to align payloads between
#             federated CMOD and federated Spectrum Protect.
#
# Project:    LA HUB
# Author:     Krzysztof Stefaniak
# Team:       TahD
# Date:       2025-08-27
# Version:    1.7 (global mode)
#
# Usage:
#   cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER]
#                     [-p PASS | --pwfile FILE] [--delim ';']
#                     [--run] [--undo]
#
# Default: DRY-RUN (prints commands, no changes).
# Global mode:
#   - One "q filespace" at start (map of all nodes/filespaces).
#   - One macro file with all valid renames.
#   - One "q filespace" at end to verify.
##############################################################################

use strict;
use warnings;
use POSIX qw(strftime);
use Text::CSV_XS;

# ---------------- CLI ----------------
my ($CSV_FILE,$SERVERNAME,$USER,$PASS,$PWFILE,$DELIM) = ("","","","","",",");
my ($DO_RUN,$UNDO) = (0,0);

while (my $arg = shift @ARGV) {
    if    ($arg eq '-c' or $arg eq '--csv')        { $CSV_FILE   = shift(@ARGV) // "" }
    elsif ($arg eq '--servername')                 { $SERVERNAME = shift(@ARGV) // "" }
    elsif ($arg eq '-u' or $arg eq '--user')       { $USER       = shift(@ARGV) // "" }
    elsif ($arg eq '-p' or $arg eq '--pass')       { $PASS       = shift(@ARGV) // "" }
    elsif ($arg eq '--pwfile')                     { $PWFILE     = shift(@ARGV) // "" }
    elsif ($arg eq '--delim')                      { $DELIM      = shift(@ARGV) // "," }
    elsif ($arg eq '--run')                        { $DO_RUN     = 1 }
    elsif ($arg eq '--undo')                       { $UNDO       = 1 }
    elsif ($arg eq '-h' or $arg eq '--help')       { print_usage(); exit 0 }
    else { die "Unknown arg: $arg (use --help)\n" }
}

die "ERROR: CSV file required (-c|--csv)\n" unless $CSV_FILE;
-f $CSV_FILE or die "ERROR: CSV not readable: $CSV_FILE\n";

# ---------------- Helpers ----------------
sub nowstamp { return strftime("%Y%m%d_%H%M%S", localtime); }
sub q_tsm { my ($s)=@_; $s //= ""; $s =~ s/"/""/g; return qq{"$s"}; }
sub clean { my ($v)=@_; $v//= ""; $v =~ s/\r$//; $v =~ s/^\s+|\s+$//g; return $v; }
sub csv_escape { my ($s)=@_; $s//= ""; if ($s =~ /[",\r\n]/){ $s =~ s/"/""/g; return qq{"$s"} } return $s; }

sub build_dsmadmc_cmd {
    my ($tsm_cmd) = @_;
    my @cmd = ("dsmadmc","-dataonly=yes","-comma","-noconfirm");
    push @cmd, "-servername=$SERVERNAME" if $SERVERNAME;
    push @cmd, "-id=$USER" if $USER;
    push @cmd, "-password=$PASS" if defined $PASS && $PASS ne "";
    push @cmd, $tsm_cmd if defined $tsm_cmd;
    return @cmd;
}
sub run_dsmadmc {
    my ($tsm_cmd) = @_;
    my @cmd = build_dsmadmc_cmd($tsm_cmd);
    my $out = qx(@cmd 2>&1); my $rc = $? >> 8;
    return ($rc,$out);
}

# ---------------- Outputs ----------------
my $stamp=nowstamp();
my $REPORT_PATH="fs_rename_report_${stamp}.csv";
my $ROLLBACK_CSV="fs_rollback_csv_${stamp}.csv";
my $BATCH_MAC="fs_batch_${stamp}.mac";

open my $rep,">:encoding(UTF-8)",$REPORT_PATH or die $!;
print $rep "node,old_fs,new_fs,action,status,message\n"; close $rep;
open my $rbc,">:encoding(UTF-8)",$ROLLBACK_CSV or die $!;
print $rbc "OD_INSTAME_SRC,AGID_NAME_SRC,OD_INSTAME_DST,AGID_NAME_DST,OD_TSM_LOGON_NAME\n"; close $rbc;

sub append_report {
    my ($node,$old,$new,$action,$status,$msg)=@_;
    open my $rfh,">>:encoding(UTF-8)",$REPORT_PATH;
    print $rfh join(",",map{csv_escape($_)}($node,$old,$new,$action,$status,$msg)),"\n"; close $rfh;
}
sub append_rollback_csv_row {
    my ($src_inst,$src_agid,$dst_inst,$dst_agid,$node)=@_;
    open my $rbfh,">>:encoding(UTF-8)",$ROLLBACK_CSV;
    print $rbfh join(",",map{csv_escape($_)}($src_inst,$src_agid,$dst_inst,$dst_agid,$node)),"\n"; close $rbfh;
}

# ---------------- Step 1: global pre-check ----------------
print STDERR "INFO : Querying all filespaces (initial snapshot)...\n";
my %fs_all;
my ($rc,$out)=run_dsmadmc("q filespace");
if($rc!=0){ die "ERROR: Cannot query filespaces: $out\n"; }

my $csv_out = Text::CSV_XS->new({ binary=>1 });
for my $line (split /\r?\n/,$out){
    next unless $csv_out->parse($line);
    my @f = $csv_out->fields;
    my ($node,$fs) = ($f[0],$f[1]);
    $fs_all{$node}{$fs} = 1 if $node && $fs;
}

# ---------------- Step 2: read input CSV ----------------
my $csv_in = Text::CSV_XS->new({ binary=>1, sep_char=>$DELIM, auto_diag=>1 });
open my $fh,"<:encoding(UTF-8)",$CSV_FILE or die $!;
my $header=$csv_in->getline($fh) or die "ERROR: Empty CSV\n";
$csv_in->column_names(@$header);

my @macro_lines;
while (my $row=$csv_in->getline_hr($fh)){
    my $node=clean($row->{OD_TSM_LOGON_NAME});
    if($node eq ""){ append_report("","","","skip","SKIPPED","OD_TSM_LOGON_NAME empty"); next; }

    my $src_inst=clean($row->{OD_INSTAME_SRC});
    my $src_agid=clean($row->{AGID_NAME_SRC});
    my $dst_inst=clean($row->{OD_INSTAME_DST});
    my $dst_agid=clean($row->{AGID_NAME_DST});

    my ($old_inst,$old_agid,$new_inst,$new_agid)=$UNDO
      ? ($dst_inst,$dst_agid,$src_inst,$src_agid)
      : ($src_inst,$src_agid,$dst_inst,$dst_agid);

    my $old_fs="/$old_inst/$old_agid"; my $new_fs="/$new_inst/$new_agid";
    my $tsm_cmd="rename filespace $node ".q_tsm($old_fs)." ".q_tsm($new_fs);

    unless($fs_all{$node}{$old_fs}){
        append_report($node,$old_fs,$new_fs,"rename","SKIPPED","old_fs not found"); next;
    }
    if($fs_all{$node}{$new_fs}){
        append_report($node,$old_fs,$new_fs,"rename","SKIPPED","new_fs already exists"); next;
    }

    if(!$DO_RUN){
        print STDERR "INFO : [DRY] $tsm_cmd\n";
        append_report($node,$old_fs,$new_fs,"rename","DRY-RUN","No changes"); next;
    }

    push @macro_lines, $tsm_cmd;
    append_report($node,$old_fs,$new_fs,"rename","PENDING","queued in macro");

    # rollback entry
    my ($rb_src_inst,$rb_src_agid,$rb_dst_inst,$rb_dst_agid)=$UNDO
      ? ($src_inst,$src_agid,$dst_inst,$dst_agid)
      : ($dst_inst,$dst_agid,$src_inst,$src_agid);
    append_rollback_csv_row($rb_src_inst,$rb_src_agid,$rb_dst_inst,$rb_dst_agid,$node);
}
close $fh;

# ---------------- Step 3: run macro ----------------
if($DO_RUN && @macro_lines){
    open my $mf,">:encoding(UTF-8)",$BATCH_MAC or die $!;
    print $mf "$_\n" for @macro_lines;
    print $mf "quit\n"; close $mf;

    print STDERR "INFO : Executing macro with ".scalar(@macro_lines)." rename commands\n";
    my @cmd=build_dsmadmc_cmd("macro $BATCH_MAC");
    my $out2=qx(@cmd 2>&1); my $rc2=$?>>8;
    print STDERR "INFO : Macro execution RC=$rc2\n";
    unlink $BATCH_MAC;
}

# ---------------- Step 4: global verify ----------------
if($DO_RUN){
    print STDERR "INFO : Querying all filespaces (post snapshot)...\n";
    my %fs_after;
    my ($rc3,$out3)=run_dsmadmc("q filespace");
    my $csv_v = Text::CSV_XS->new({ binary=>1 });
    for my $line (split /\r?\n/,$out3){
        next unless $csv_v->parse($line);
        my @f=$csv_v->fields; my ($node,$fs)=($f[0],$f[1]);
        $fs_after{$node}{$fs}=1 if $node && $fs;
    }

    # update report with OK/WARN
    open my $rfh,"<:encoding(UTF-8)",$REPORT_PATH or die $!;
    my @lines=<$rfh>; close $rfh;
    open my $wf,">:encoding(UTF-8)",$REPORT_PATH or die $!;
    print $wf $lines[0]; # header
    for my $l(@lines[1..$#lines]){
        chomp($l);
        my @f=split/,/,$l;
        if($f[3] eq "rename" && $f[4] eq "PENDING"){
            my ($node,$old,$new)=($f[0],$f[1],$f[2]);
            if($fs_after{$node}{$new} && !$fs_after{$node}{$old}){
                $f[4]="OK"; $f[5]="renamed; verified";
            } elsif($fs_after{$node}{$new} && $fs_after{$node}{$old}){
                $f[4]="WARN"; $f[5]="both old and new present";
            } else {
                $f[4]="FAILED"; $f[5]="rename not applied";
            }
            $l=join(",",@f);
        }
        print $wf "$l\n";
    }
    close $wf;
}

print STDERR "INFO : Done. Report: $REPORT_PATH\n";
print STDERR "INFO : Rollback CSV: $ROLLBACK_CSV\n";

sub print_usage {
    print <<'USAGE';
Usage:
  cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER] [-p PASS | --pwfile FILE]
                    [--delim ';'] [--run] [--undo]

Default: dry-run (no changes).
Global mode:
  - One "q filespace" at start.
  - One macro file with all renames.
  - One "q filespace" at end for verify.
USAGE
}