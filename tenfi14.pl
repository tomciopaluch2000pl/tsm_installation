#!/usr/bin/env perl
##############################################################################
# Script:     cmod_fs_rename.pl
# Purpose:    Rename Spectrum Protect filespaces to align payloads between
#             federated CMOD (Content Manager OnDemand) and federated
#             Spectrum Protect, ensuring proper linkage of archived payloads
#             after federation.
#
# Project:    LA HUB
# Author:     Krzysztof Stefaniak
# Team:       TahD
# Date:       2025-08-27
# Version:    1.5 (batch mode)
#
# Usage:
#   cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER]
#                     [-p PASS | --pwfile FILE] [--delim ';']
#                     [--run] [--verify] [--undo]
#                     [--no-batch] [--keep-batch]
#
# Default: DRY-RUN (prints commands, no changes).
# Batch mode:
#   - Pre-checks: one "q filespace" per node, build cache.
#   - Generates macro file with all valid renames.
#   - Runs one dsmadmc session with macro.
#   - Optionally verifies per node at the end.
#
# Options:
#   --no-batch   : disable batch, run legacy slow mode (1 rename per dsmadmc)
#   --keep-batch : do not delete generated macro file (for debugging)
#
##############################################################################

use strict;
use warnings;
use POSIX qw(strftime);
use Text::CSV_XS;

my ($CSV_FILE,$SERVERNAME,$USER,$PASS,$PWFILE,$DELIM) = ("","","","","",",");
my ($DO_RUN,$VERIFY,$UNDO) = (0,0,0);
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
    elsif ($arg eq '--undo')                       { $UNDO       = 1 }
    elsif ($arg eq '--no-batch')                   { $BATCH      = 0 }
    elsif ($arg eq '--keep-batch')                 { $KEEP_BATCH = 1 }
    elsif ($arg eq '-h' or $arg eq '--help')       { print_usage(); exit 0 }
    else { die "Unknown arg: $arg (use --help)\n" }
}

die "ERROR: CSV file required (-c|--csv)\n" unless $CSV_FILE;
-f $CSV_FILE or die "ERROR: CSV not readable: $CSV_FILE\n";

# ---------------- Helpers ----------------
sub nowstamp { return strftime("%Y%m%d_%H%M%S", localtime); }
sub q_tsm { my ($s)=@_; $s //= ""; $s =~ s/"/""/g; return qq{"$s"}; }
sub human_bytes {
    my ($b) = @_; $b ||= 0; my @u = ('B','KB','MB','GB','TB'); my $i=0;
    while ($b >= 1024 && $i < $#u) { $b/=1024; $i++ }
    return $i==0 ? sprintf('%d %s',$b,$u[$i]) : sprintf('%.1f %s',$b,$u[$i]);
}
sub clean { my ($v)=@_; $v//= ""; $v =~ s/\r$//; $v =~ s/^\s+|\s+$//g; return $v; }
sub csv_escape { my ($s)=@_; $s//= ""; if ($s =~ /[",\r\n]/){ $s =~ s/"/""/g; return qq{"$s"} } return $s; }

sub build_dsmadmc_cmd {
    my ($tsm_cmd) = @_;
    my @cmd = ("dsmadmc", "-dataonly=yes", "-comma", "-noconfirm");
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
    return ($rc, $out, "");
}
sub fs_list_for_node {
    my ($node) = @_;
    my ($rc, $out, $err) = run_dsmadmc("q filespace $node");
    return () if $rc != 0;
    my @names; my $csv = Text::CSV_XS->new({ binary=>1 });
    for my $line (split /\r?\n/, $out) {
        next unless length $line;
        if ($csv->parse($line)) { my @f = $csv->fields; push @names, $f[1] if @f >= 2; }
    }
    return @names;
}

# ---------------- Disk space precheck ----------------
sub check_disk_space_for_outputs {
    my ($input_csv) = @_;
    my $input_size = -s $input_csv; my $need_bytes = int($input_size*2.2);
    my $out_dir = "."; my $os=`uname -s`; chomp($os);
    my $df = `df -Pk $out_dir 2>/dev/null`; my @lines = split /\n/, $df;
    return if @lines<2;
    my @f=split /\s+/, $lines[-1]; my $avail_kb = ($os eq 'AIX')? $f[2]:$f[3];
    $avail_kb=0 unless defined $avail_kb && $avail_kb =~ /^\d+$/;
    my $avail_bytes=$avail_kb*1024;
    if ($avail_bytes<$need_bytes){ die sprintf("ERROR: Not enough space. Need %s, avail %s\n",human_bytes($need_bytes),human_bytes($avail_bytes)); }
    printf STDERR "INFO : Disk space OK (need ~%s, avail %s).\n",human_bytes($need_bytes),human_bytes($avail_bytes);
}

check_disk_space_for_outputs($CSV_FILE);

# ---------------- Password resolution ----------------
if ($PWFILE && !$PASS) {
    open my $pfh,"<:encoding(UTF-8)",$PWFILE or die "ERROR: Cannot read $PWFILE: $!\n";
    chomp($PASS=<$pfh>//""); close $pfh;
}
if ($DO_RUN && $USER && !$PASS) {
    print STDERR "Enter password for '$USER': "; system("stty -echo");
    chomp($PASS=<STDIN>//""); system("stty echo"); print STDERR "\n";
}

# ---------------- Outputs ----------------
my $stamp=nowstamp();
my $REPORT_PATH="fs_rename_report_${stamp}.csv";
my $ROLLBACK_CSV="fs_rollback_csv_${stamp}.csv";
my $BATCH_MAC="fs_batch_${stamp}.mac";

open my $rep,">:encoding(UTF-8)",$REPORT_PATH or die "Cannot write $REPORT_PATH: $!";
print $rep "node,old_fs,new_fs,action,status,message\n"; close $rep;
open my $rbc,">:encoding(UTF-8)",$ROLLBACK_CSV or die "Cannot write $ROLLBACK_CSV: $!";
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

# ---------------- Main ----------------
my $csv = Text::CSV_XS->new({ binary=>1, sep_char=>$DELIM, auto_diag=>1 });
open my $fh,"<:encoding(UTF-8)",$CSV_FILE or die "ERROR: Cannot read $CSV_FILE: $!\n";
my $header=$csv->getline($fh) or die "ERROR: Empty CSV\n"; $csv->column_names(@$header);

my %idx; for my $i (0..$#$header){$idx{$header->[$i]}=$i;}
for my $need(qw/OD_INSTAME_SRC AGID_NAME_SRC OD_INSTAME_DST AGID_NAME_DST OD_TSM_LOGON_NAME/){
    die "ERROR: Missing $need\n" unless exists $idx{$need};
}

my %fs_cache; my %batch_cmds;

while (my $row=$csv->getline_hr($fh)){
    my $node=clean($row->{OD_TSM_LOGON_NAME});
    if($node eq ""){ append_report("","","","skip","SKIPPED","OD_TSM_LOGON_NAME empty"); next; }
    my $src_inst=clean($row->{OD_INSTAME_SRC});
    my $src_agid=clean($row->{AGID_NAME_SRC});
    my $dst_inst=clean($row->{OD_INSTAME_DST});
    my $dst_agid=clean($row->{AGID_NAME_DST});

    my ($old_inst,$old_agid,$new_inst,$new_agid)=$UNDO?($dst_inst,$dst_agid,$src_inst,$src_agid):($src_inst,$src_agid,$dst_inst,$dst_agid);

    my $old_fs="/$old_inst/$old_agid"; my $new_fs="/$new_inst/$new_agid";
    my $tsm_cmd="rename filespace $node ".q_tsm($old_fs)." ".q_tsm($new_fs);

    # Cache load
    unless (exists $fs_cache{$node}) {
        my %set=map{$_=>1} fs_list_for_node($node);
        $fs_cache{$node}=\%set;
    }

    # Pre-checks
    unless($fs_cache{$node}->{$old_fs}){ append_report($node,$old_fs,$new_fs,"rename","SKIPPED","old_fs not found"); next; }
    if($fs_cache{$node}->{$new_fs}){ append_report($node,$old_fs,$new_fs,"rename","SKIPPED","new_fs exists"); next; }

    if(!$DO_RUN){
        print STDERR "INFO : [DRY] $tsm_cmd\n";
        append_report($node,$old_fs,$new_fs,"rename","DRY-RUN","No changes");
        next;
    }

    if($BATCH){
        push @{ $batch_cmds{$node} }, [$tsm_cmd,$node,$old_fs,$new_fs,$src_inst,$src_agid,$dst_inst,$dst_agid];
        delete $fs_cache{$node}->{$old_fs}; $fs_cache{$node}->{$new_fs}=1; # optimistic
    } else {
        print STDERR "INFO : Executing: $tsm_cmd\n";
        my ($rc,$out,$err)=run_dsmadmc($tsm_cmd);
        if($rc!=0){ my $msg=$out; $msg=~s/[\r\n,]/ /g; append_report($node,$old_fs,$new_fs,"rename","FAILED",$msg); next; }
        append_report($node,$old_fs,$new_fs,"rename","OK","renamed");
        append_rollback_csv_row($new_inst,$new_agid,$old_inst,$old_agid,$node);
    }
}
close $fh;

if($DO_RUN && $BATCH){
    open my $mf,">:encoding(UTF-8)",$BATCH_MAC or die "Cannot write $BATCH_MAC: $!";
    for my $node(keys %batch_cmds){ for my $c(@{ $batch_cmds{$node} }){ print $mf $c->[0],"\n"; } }
    print $mf "quit\n"; close $mf;

    print STDERR "INFO : Executing batch macro $BATCH_MAC\n";
    my @cmd=build_dsmadmc_cmd("macro $BATCH_MAC");
    my $out=qx(@cmd 2>&1); my $rc=$?>>8;
    for my $node(keys %batch_cmds){
        for my $c(@{ $batch_cmds{$node} }){
            my ($cmd,$n,$old,$new,$src_inst,$src_agid,$dst_inst,$dst_agid)=@$c;
            if($out=~/\Q$old\E.*\Q$new\E/ && $out=~/successfully/i){
                my $status="OK"; my $msg="renamed";
                if($VERIFY){ my %set=map{$_=>1} fs_list_for_node($n); $status=($set{$new}&&!$set{$old})?"OK":"WARN"; $msg.="; verified"; }
                append_report($n,$old,$new,"rename",$status,$msg);
                append_rollback_csv_row($new_inst,$dst_agid,$old_inst,$old_agid,$n);
            } else {
                append_report($n,$old,$new,"rename","FAILED","see macro output");
            }
        }
    }
    unlink $BATCH_MAC unless $KEEP_BATCH;
}

print STDERR "INFO : Done. Report: $REPORT_PATH\n";
print STDERR "INFO : Rollback CSV: $ROLLBACK_CSV\n";

sub print_usage {
    print <<'USAGE';
Usage:
  cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER] [-p PASS | --pwfile FILE]
                    [--delim ';'] [--run] [--verify] [--undo]
                    [--no-batch] [--keep-batch]
USAGE
}