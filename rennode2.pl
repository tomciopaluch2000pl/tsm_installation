#!/usr/bin/env perl
# =====================================================================
#  Name:        sp_rename_nodes_sql.pl
#  Purpose:     Bulk-rename Spectrum Protect nodes by adding a prefix,
#               and (post-rename) set SESSIONSECURITY=TRANSITIONAL to
#               ensure CMOD's first contact after renaming.
#
#  Author:      Krzysztof Stefaniak
#  Team:        TahD
# =====================================================================

use strict;
use warnings;
use Getopt::Long qw(GetOptions);
use IPC::Open3;
use Symbol qw(gensym);
use File::Spec;
use POSIX qw(strftime);

my $dsmadmc_bin = $ENV{DSMADMC_PATH} || 'dsmadmc';
my ($id, $pass, $server);
my $prefix       = '';
my $out_dir      = '.';
my $no_exec      = 0;
my $dry_run      = 0;
my $upper        = 0;
my $skip_pref    = 1;
my $only_regex   = '';
my $exclude_regex= '';
my $ask_confirm  = 0;
my $help         = 0;

GetOptions(
  'id=s'            => \$id,
  'pass=s'          => \$pass,
  'server=s'        => \$server,
  'prefix=s'        => \$prefix,        # may be 'N1' or 'N1-'
  'out-dir=s'       => \$out_dir,
  'dsmadmc=s'       => \$dsmadmc_bin,
  'no-exec'         => \$no_exec,
  'preview|dry-run' => \$dry_run,
  'upper'           => \$upper,
  'skip-prefixed!'  => \$skip_pref,
  'only-regex=s'    => \$only_regex,
  'exclude-regex=s' => \$exclude_regex,
  'ask-confirm'     => \$ask_confirm,
  'help'            => \$help,
) or die "Invalid options. Use --help\n";

if ($help) {
  print <<"USAGE";
Usage:
  $0 --id ADMIN --pass PASS --server ALIAS --prefix N1[-] [options]
USAGE
  exit 0;
}

die "--prefix is required (e.g. N1 or N1-)\n" unless defined $prefix && length $prefix;

# NEW: normalize prefix to always end with a single dash
$prefix =~ s/-+$//;        # strip trailing dashes if any
$prefix .= '-';            # then ensure exactly one dash

die "Output directory does not exist: $out_dir\n" unless -d $out_dir;

sub run_dsmadmc {
  my (@tail) = @_;
  my @cmd = ($dsmadmc_bin);
  push @cmd, "-se=$server"     if defined $server;
  push @cmd, "-id=$id"         if defined $id;
  push @cmd, "-password=$pass" if defined $pass;
  push @cmd, "-noc"            unless $ask_confirm;  # use -noc as requested
  push @cmd, @tail;

  my $err = gensym;
  my $pid = open3(undef, my $out, $err, @cmd);
  my @stdout = <$out>;
  my @stderr = <$err>;
  waitpid($pid, 0);
  my $rc = $? >> 8;
  return ($rc, \@stdout, \@stderr, \@cmd);
}

sub ts { strftime('%Y%m%d_%H%M%S', localtime) }
sub build_new_name {
  my ($old) = @_;
  my $new = $prefix . $old;
  $new = uc($new) if $upper;
  return $new;
}

# Fetch nodes via SQL (compact & fast)
my $sql = q{select node_name, session_security from nodes};
my ($rc_sel, $out_sel, $err_sel, $cmd_sel) = run_dsmadmc($sql);
die "SELECT failed (rc=$rc_sel)\nCMD: @{$cmd_sel}\n".join('',@$err_sel) if $rc_sel != 0;

# Parse output (robust to different formats)
my @rows; my ($cur_name, $cur_sec);
for my $line (@$out_sel) {
  chomp $line; $line =~ s/\r$//; next if $line =~ /^\s*$/;

  if ($line =~ /^\s*NODE_NAME\s*:\s*(.+?)\s*$/i) { $cur_name = $1; next; }
  if ($line =~ /^\s*SESSION_SECURITY\s*:\s*(.+?)\s*$/i) {
    $cur_sec = $1; push @rows, [$cur_name, $cur_sec] if defined $cur_name;
    ($cur_name, $cur_sec) = (undef, undef); next;
  }
  if ($line =~ /^\s*([^,]+)\s*,\s*([^,]+)\s*$/) { push @rows, [$1,$2]; next; }
  if ($line =~ /^\s*(\S+)\s+(\S.*?)\s*$/)       { push @rows, [$1,$2]; next; }
}
die "No rows parsed from SELECT output.\n" unless @rows;

my %nodes;
for my $r (@rows) {
  my ($n,$s) = @$r; next unless defined $n && $n ne '';
  $n =~ s/^\s+|\s+$//g; $s = '' unless defined $s; $s =~ s/^\s+|\s+$//g;
  $nodes{$n} = $s;
}
die "Empty node list.\n" unless %nodes;

my @all = sort keys %nodes;
if ($only_regex)   { my $re = qr/$only_regex/;   @all = grep { $_ =~ $re } @all; }
if ($exclude_regex){ my $re = qr/$exclude_regex/;@all = grep { $_ !~ $re } @all; }

my %exists = map { $_=>1 } keys %nodes;
my @plan;
for my $old (@all) {
  if ($skip_pref && index($old, $prefix) == 0) {
    push @plan, { old=>$old, new=>'', status=>'skipped_prefixed', reason=>'already has prefix' };
    next;
  }
  my $new = build_new_name($old);
  if ($exists{$new}) {
    push @plan, { old=>$old, new=>$new, status=>'conflict', reason=>'target exists' };
    continue;
  }
  push @plan, { old=>$old, new=>$new, status=>'planned', reason=>'' };
}

my $stamp = ts();
my $snapshot_csv = File::Spec->catfile($out_dir, "snapshot_nodes_${stamp}.csv");
my $report_csv   = File::Spec->catfile($out_dir, "rename_report_${stamp}.csv");

open my $snap, '>', $snapshot_csv or die "Cannot write $snapshot_csv: $!\n";
print $snap "node_name,session_security\n";
for my $n (sort keys %nodes) { printf $snap "%s,%s\n", $n, ($nodes{$n}//''); }
close $snap;

open my $rpt, '>', $report_csv or die "Cannot write $report_csv: $!\n";
print $rpt "old_name,new_name,status,reason\n";
for my $row (@plan) {
  printf $rpt "%s,%s,%s,%s\n",
    ($row->{old}//''), ($row->{new}//''), ($row->{status}//''), ($row->{reason}//'');
}
close $rpt;

if ($dry_run) {
  my $todo = scalar grep { $_->{status} eq 'planned' } @plan;
  print "Preview: would process $todo nodes.\nSnapshot: $snapshot_csv\nReport: $report_csv\n";
  exit 0;
}

my $macro_do   = File::Spec->catfile($out_dir, "rename_and_update_${stamp}.mac");
my $macro_undo = File::Spec->catfile($out_dir, "rollback_${stamp}.mac");

open my $mdo, '>', $macro_do or die "Cannot write $macro_do: $!\n";
print $mdo "/* Macro $stamp: rename + set SESSIONSECURITY=TRANSITIONAL */\n";
for my $row (grep { $_->{status} eq 'planned' } @plan) {
  printf $mdo "rename node \"%s\" \"%s\"\n", $row->{old}, $row->{new};
  printf $mdo "update node \"%s\" sessionsecurity=transitional\n", $row->{new};
}
close $mdo;

open my $mrb, '>', $macro_undo or die "Cannot write $macro_undo: $!\n";
print $mrb "/* Macro $stamp: rollback ONLY names; keep SESSIONSECURITY=TRANSITIONAL */\n";
# NEW: only rename back; do NOT restore sessionsecurity
for my $row (reverse grep { $_->{status} eq 'planned' } @plan) {
  printf $mrb "rename node \"%s\" \"%s\"\n", $row->{new}, $row->{old};
}
close $mrb;

if ($no_exec) {
  print "Artifacts ready:\n  Macro (do):      $macro_do\n  Macro (rollback):$macro_undo\n  Report:          $report_csv\n  Snapshot:        $snapshot_csv\n";
  exit 0;
}

my $logfile = File::Spec->catfile($out_dir, "session_${stamp}.log");
my ($rc_run, $out_run, $err_run, $cmd_run) = run_dsmadmc('macro "'.$macro_do.'"');
open my $log, '>', $logfile or die "Cannot write $logfile: $!\n";
print $log "CMD: @{$cmd_run}\n\n--- STDOUT ---\n", @$out_run, "\n--- STDERR ---\n", @$err_run;
close $log;

if ($rc_run != 0) {
  warn "Macro rc=$rc_run. Check log: $logfile\nRollback macro: $macro_undo\n";
  exit $rc_run;
}

print "Success. Changes applied.\nRollback macro: $macro_undo\nReport: $report_csv\nSnapshot: $snapshot_csv\nLog: $logfile\n";
exit 0;