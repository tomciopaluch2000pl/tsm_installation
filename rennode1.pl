#!/usr/bin/env perl
# =====================================================================
#  Name:        sp_rename_nodes_sql.pl
#  Purpose:     Bulk-rename Spectrum Protect nodes by adding a prefix,
#               and (post-rename) set SESSIONSECURITY=TRANSITIONAL to
#               ensure CMOD's first contact after renaming.
#
#  Inputs:      Connects to the SP server using dsmadmc and runs:
#                  SELECT node_name, session_security FROM nodes
#               Then it generates two macros:
#                 - rename_and_update_<timestamp>.mac  (exec plan)
#                 - rollback_<timestamp>.mac           (undo)
#               Also produces CSV reports and a session log.
#
#  Author:      Krzysztof Stefaniak
#  Team:        TahD
#  License:     Internal use. Share responsibly.
#
#  Notes:
#    * We use SQL (SELECT) instead of "q node f=d" to minimize output.
#    * Column names follow IBM Storage Protect schema:
#         NODE_NAME, SESSION_SECURITY (values include Transitional/Strict).
#    * We run dsmadmc with -NOCONFIRM (aka -noc) to avoid Y/N prompts.
#    * Macro executes all changes in ONE admin session for atomic feel.
#    * Rollback restores both the original name and the original
#      SESSION_SECURITY value per node.
#    * Security: Passing -password on CLI may expose secrets via 'ps'.
#      Prefer credentials file or environment if possible.
# =====================================================================

use strict;
use warnings;
use Getopt::Long qw(GetOptions);
use IPC::Open3;
use Symbol qw(gensym);
use File::Spec;
use POSIX qw(strftime);

# ------------------------- CLI OPTIONS -------------------------------

my $dsmadmc_bin = $ENV{DSMADMC_PATH} || 'dsmadmc';
my ($id, $pass, $server);
my $prefix       = '';
my $out_dir      = '.';
my $no_exec      = 0;         # generate artifacts but do not run macro
my $dry_run      = 0;         # preview stats only (no macro/log)
my $upper        = 0;         # force uppercase for new names
my $skip_pref    = 1;         # skip already prefixed nodes
my $only_regex   = '';
my $exclude_regex= '';
my $ask_confirm  = 0;         # if set, do NOT pass -noc to dsmadmc
my $help         = 0;

GetOptions(
  'id=s'            => \$id,
  'pass=s'          => \$pass,
  'server=s'        => \$server,        # dsmadmc -se=<server>
  'prefix=s'        => \$prefix,        # e.g., N1-
  'out-dir=s'       => \$out_dir,
  'dsmadmc=s'       => \$dsmadmc_bin,
  'no-exec'         => \$no_exec,
  'preview|dry-run' => \$dry_run,
  'upper'           => \$upper,
  'skip-prefixed!'  => \$skip_pref,
  'only-regex=s'    => \$only_regex,
  'exclude-regex=s' => \$exclude_regex,
  'ask-confirm'     => \$ask_confirm,   # override -noc
  'help'            => \$help,
) or die "Invalid options. Use --help\n";

if ($help) {
  print <<"USAGE";
Usage:
  $0 --id ADMIN --pass PASS --server ALIAS --prefix N1- [options]

Key options:
  --id, --pass, --server    Login to dsmadmc (server is -se=)
  --prefix N1-              Prefix to add (SHOULD end with '-')
  --out-dir DIR             Folder for macros/reports (default: .)
  --dsmadmc /path/dsmadmc   Path to dsmadmc (or DSMADMC_PATH)
  --no-exec                 Generate macros & reports, don't execute
  --preview                 Print what would change; no files created
  --upper                   Force UPPERCASE for new names
  --skip-prefixed / --no-skip-prefixed  Skip names already starting with prefix
  --only-regex 're'         Act only on nodes matching regex
  --exclude-regex 're'      Exclude nodes matching regex
  --ask-confirm             Do not pass -noc (default uses -noc)
USAGE
  exit 0;
}

die "--prefix is required and should end with '-', e.g. N1-\n"
  unless defined $prefix && $prefix =~ /-\z/;
die "Output directory does not exist: $out_dir\n" unless -d $out_dir;

# ------------------------- HELPERS -----------------------------------

sub run_dsmadmc {
  my (@command_tail) = @_;
  my @cmd = ($dsmadmc_bin);

  push @cmd, "-se=$server"    if defined $server;
  push @cmd, "-id=$id"        if defined $id;
  push @cmd, "-password=$pass" if defined $pass; # consider credentials file for security
  push @cmd, "-noc"           unless $ask_confirm;  # avoid interactive prompts
  # Keep output raw for SELECT; we'll parse ourselves.
  push @cmd, @command_tail;

  my $err = gensym;
  my $pid = open3(undef, my $out, $err, @cmd);
  my @stdout = <$out>;
  my @stderr = <$err>;
  waitpid($pid, 0);
  my $rc = $? >> 8;

  return ($rc, \@stdout, \@stderr, \@cmd);
}

sub ts {
  return strftime('%Y%m%d_%H%M%S', localtime);
}

sub build_new_name {
  my ($old) = @_;
  my $new = $prefix . $old;
  $new = uc($new) if $upper;
  return $new;
}

# ------------------------- FETCH NODES VIA SQL -----------------------

# We select exactly the two columns we need (fast, compact).
my $sql = q{select node_name, session_security from nodes};
my ($rc_sel, $out_sel, $err_sel, $cmd_sel) = run_dsmadmc($sql);
if ($rc_sel != 0) {
  die "dsmadmc SELECT failed (rc=$rc_sel)\nCMD: @{$cmd_sel}\nSTDERR:\n".join('', @$err_sel);
}

# Parse SELECT output. We accept both "COLUMN: value" style and tabular text.
# Heuristics:
#   1) Lines containing "NODE_NAME:" and "SESSION_SECURITY:" (colon style)
#   2) Or split by whitespace when two tokens look plausible
#   3) Or CSV-like "value1,value2" (depending on locale/options)
my @rows;
my ($cur_name, $cur_sec);
for my $line (@$out_sel) {
  chomp($line);
  $line =~ s/\r$//;
  next if $line =~ /^\s*$/;

  if ($line =~ /^\s*NODE_NAME\s*:\s*(.+?)\s*$/i) {
    $cur_name = $1;
    next;
  }
  if ($line =~ /^\s*SESSION_SECURITY\s*:\s*(.+?)\s*$/i) {
    $cur_sec = $1;
    if (defined $cur_name) {
      push @rows, [$cur_name, $cur_sec];
      ($cur_name, $cur_sec) = (undef, undef);
    }
    next;
  }

  # Try CSV (two fields)
  if ($line =~ /^\s*([^,]+)\s*,\s*([^,]+)\s*$/) {
    push @rows, [$1, $2];
    next;
  }

  # Try two columns separated by whitespace (last fallback)
  if ($line =~ /^\s*(\S+)\s+(\S.*?)\s*$/) {
    push @rows, [$1, $2];
    next;
  }
}

die "Could not parse any rows from SELECT output. Check locale/format.\n"
  unless @rows;

# Normalize & map
my %nodes;            # node_name -> session_security
for my $r (@rows) {
  my ($n, $s) = @$r;
  next unless defined $n && $n ne '';
  $n =~ s/^\s+|\s+$//g;
  $s = defined($s) ? $s : '';
  $s =~ s/^\s+|\s+$//g;
  $nodes{$n} = $s;
}

die "Empty node list after parsing.\n" unless %nodes;

# ------------------------- FILTERS & PLAN ----------------------------

my @all_nodes = sort keys %nodes;

# Optional filters
if ($only_regex) {
  my $re = qr/$only_regex/;
  @all_nodes = grep { $_ =~ $re } @all_nodes;
}
if ($exclude_regex) {
  my $re = qr/$exclude_regex/;
  @all_nodes = grep { $_ !~ $re } @all_nodes;
}

# Build existing set and plan
my %exists = map { $_ => 1 } sort keys %nodes;
my @plan;  # { old, new, old_sec, status, reason }
for my $old (@all_nodes) {
  if ($skip_pref && index($old, $prefix) == 0) {
    push @plan, { old=>$old, new=>'', old_sec=>$nodes{$old}, status=>'skipped_prefixed', reason=>'already has prefix' };
    next;
  }
  my $new = build_new_name($old);
  if ($exists{$new}) {
    push @plan, { old=>$old, new=>$new, old_sec=>$nodes{$old}, status=>'conflict', reason=>'target name exists' };
    next;
  }
  push @plan, { old=>$old, new=>$new, old_sec=>$nodes{$old}, status=>'planned', reason=>'' };
}

# ------------------------- REPORTS & SNAPSHOT ------------------------

my $stamp = ts();
my $snapshot_csv = File::Spec->catfile($out_dir, "snapshot_nodes_${stamp}.csv");
my $report_csv   = File::Spec->catfile($out_dir, "rename_report_${stamp}.csv");
open my $snap, '>', $snapshot_csv or die "Cannot write $snapshot_csv: $!\n";
print $snap "node_name,session_security\n";
for my $n (sort keys %nodes) {
  printf $snap "%s,%s\n", $n, ($nodes{$n} // '');
}
close $snap;

open my $rpt, '>', $report_csv or die "Cannot write $report_csv: $!\n";
print $rpt "old_name,new_name,old_session_security,status,reason\n";
for my $row (@plan) {
  printf $rpt "%s,%s,%s,%s,%s\n",
    ($row->{old}//''), ($row->{new}//''), ($row->{old_sec}//''), ($row->{status}//''), ($row->{reason}//'');
}
close $rpt;

if ($dry_run) {
  my $todo = scalar grep { $_->{status} eq 'planned' } @plan;
  print "Preview: would process $todo nodes.\n";
  print "Snapshot: $snapshot_csv\nReport:   $report_csv\n";
  exit 0;
}

# ------------------------- MACROS (DO & ROLLBACK) --------------------

my $macro_do  = File::Spec->catfile($out_dir, "rename_and_update_${stamp}.mac");
my $macro_undo= File::Spec->catfile($out_dir, "rollback_${stamp}.mac");

open my $mdo, '>', $macro_do or die "Cannot write $macro_do: $!\n";
print $mdo "/* Macro generated $stamp - rename + set SESSIONSECURITY=TRANSITIONAL */\n";
for my $row (grep { $_->{status} eq 'planned' } @plan) {
  printf $mdo "rename node \"%s\" \"%s\"\n", $row->{old}, $row->{new};
  printf $mdo "update node \"%s\" sessionsecurity=transitional\n", $row->{new};
}
close $mdo;

open my $mrb, '>', $macro_undo or die "Cannot write $macro_undo: $!\n";
print $mrb "/* Macro generated $stamp - rollback to original name & SESSION_SECURITY */\n";
# Reverse order just in case of dependencies
for my $row (reverse grep { $_->{status} eq 'planned' } @plan) {
  # Restore original name
  printf $mrb "rename node \"%s\" \"%s\"\n", $row->{new}, $row->{old};
  # Restore original session security if we know it
  if (defined $row->{old_sec} && $row->{old_sec} ne '') {
    my $val = lc($row->{old_sec}) eq 'transitional' ? 'transitional'
            : lc($row->{old_sec}) eq 'strict'       ? 'strict'
            : $row->{old_sec}; # leave as-is if non-standard (future-proof)
    printf $mrb "update node \"%s\" sessionsecurity=%s\n", $row->{old}, $val;
  }
}
close $mrb;

# ------------------------- EXECUTION (ONE SESSION) -------------------

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
  warn "Macro execution returned rc=$rc_run. Check log: $logfile\n";
  warn "Rollback macro: $macro_undo\n";
  exit $rc_run;
}

print "Success. Changes applied.\n";
print "Rollback macro: $macro_undo\nReport: $report_csv\nSnapshot: $snapshot_csv\nLog: $logfile\n";
exit 0;