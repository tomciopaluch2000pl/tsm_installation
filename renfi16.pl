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
# Version:    1.6 (verify-late)
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
use Text::CSV_XS;
use Cwd qw(abs_path);

# ---------------- CLI ----------------
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
sub fs_list_for_node {
    my ($node) = @_;
    my ($rc,$out)=run_dsmadmc("q filespace $node"); return () if $rc!=0;
    my @names; my $csv = Text::CSV_XS->new({ binary=>1 });
    for my $line (split /\r?\n/,$out){
        next unless length $line;
        if ($csv->parse($line)){ my @f=$csv->fields; push @names,$f[1] if @f>=2; }
    }
    return @names;
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

# ---------------- Main ----------------
my $csv = Text::CSV_XS->new({ binary=>1, sep_char=>$DELIM, auto_diag=>1 });
open my $fh,"<:encoding(UTF-8)",$CSV_FILE or die $!;
my $header=$csv->getline($fh) or die "ERROR: Empty CSV\n"; $csv->column_names(@$header);

my %fs_cache; my %batch_cmds; my %verify_late_nodes;

while (my $row=$csv->getline_hr($fh)){
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

    unless(exists $fs_cache{$node}){
        my %set=map{$_=>1} fs_list_for_node($node);
        $fs_cache{$node}=\%set;
    }

    unless($fs_cache{$node}->{$old_fs}){ append_report($node,$old_fs,$new_fs,"rename","SKIPPED","old_fs not found"); next; }
    if($fs_cache{$node}->{$new_fs}){ append_report($node,$old_fs,$new_fs,"rename","SKIPPED","new_fs exists"); next; }

    if(!$DO_RUN){ print STDERR "INFO : [DRY] $tsm_cmd\n"; append_report($node,$old_fs,$new_fs,"rename","DRY-RUN","No changes"); next; }

    if($BATCH){
        push @{ $batch_cmds{$node} }, [ $tsm_cmd,$node,$old_fs,$new_fs,$src_inst,$src_agid,$dst_inst,$dst_agid ];
        $fs_cache{$node}->{$new_fs}=1; delete $fs_cache{$node}->{$old_fs};
        $verify_late_nodes{$node}++ if $VERIFY_LATE;
    } else {
        print STDERR "INFO : Executing: $tsm_cmd\n";
        my ($rc,$out)=run_dsmadmc($tsm_cmd);
        if($rc!=0){ append_report($node,$old_fs,$new_fs,"rename","FAILED","$out"); next; }
        if($VERIFY && !$VERIFY_LATE){
            my %set=map{$_=>1} fs_list_for_node($node);
            my $status=($set{$new_fs}&&!$set{$old_fs})?"OK":"WARN";
            append_report($node,$old_fs,$new_fs,"rename",$status,"renamed; verified");
        } else {
            append_report($node,$old_fs,$new_fs,"rename","OK","renamed");
            $verify_late_nodes{$node}++ if $VERIFY_LATE;
        }
    }
}
close $fh;

# --- batch execution ---
if($DO_RUN && $BATCH){
    open my $mf,">:encoding(UTF-8)",$BATCH_MAC or die $!;
    for my $node(keys %batch_cmds){ for my $c(@{ $batch_cmds{$node} }){ print $mf $c->[0],"\n"; } }
    print $mf "quit\n"; close $mf;
    print STDERR "INFO : Executing macro $BATCH_MAC\n";
    my @cmd=build_dsmadmc_cmd("macro $BATCH_MAC"); my $out=qx(@cmd 2>&1);
    for my $node(keys %batch_cmds){
        for my $c(@{ $batch_cmds{$node} }){
            my ($cmd,$n,$old,$new)=@$c;
            append_report($n,$old,$new,"rename","OK","renamed (macro)") unless $VERIFY_LATE;
            $verify_late_nodes{$n}++ if $VERIFY_LATE;
        }
    }
    unlink $BATCH_MAC unless $KEEP_BATCH;
}

# --- late verify ---
if($DO_RUN && $VERIFY_LATE){
    for my $node(keys %verify_late_nodes){
        my %set=map{$_=>1} fs_list_for_node($node);
        open my $rfh,"<:encoding(UTF-8)","$REPORT_PATH" or next;
        my @lines=<$rfh>; close $rfh;
        open my $wf,">:encoding(UTF-8)","$REPORT_PATH" or next;
        print $wf $lines[0]; # header
        for my $l(@lines[1..$#lines]){
            chomp($l);
            my @f=split/,/,$l; # simplistic split (OK for report format)
            if($f[0] eq $node && $f[3] eq "rename" && $f[4] eq "OK"){
                my ($old,$new)=($f[1],$f[2]);
                if($set{$new} && !$set{$old}){ $f[4]="OK"; $f[5].="; verified late"; }
                else{ $f[4]="WARN"; $f[5].="; late verify mismatch"; }
                $l=join(",",@f);
            }
            print $wf "$l\n";
        }
        close $wf;
    }
}

print STDERR "INFO : Done. Report: $REPORT_PATH\n";
print STDERR "INFO : Rollback CSV: $ROLLBACK_CSV\n";

sub print_usage {
    print <<'USAGE';
Usage:
  cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER] [-p PASS | --pwfile FILE]
                    [--delim ';'] [--run] [--verify | --verify-late] [--undo]
                    [--no-batch] [--keep-batch]
USAGE
}