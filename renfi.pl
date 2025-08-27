#!/usr/bin/env perl
use strict;
use warnings;
use POSIX qw(strftime);
use IO::Handle;
use IPC::Open3;
use Symbol qw(gensym);

##############################################################################
# CSV parsing: prefer Text::CSV_XS (fast C implementation); fall back to
# Text::CSV if XS is not installed on the server.
##############################################################################
my $Have_CSV_XS = 0;
BEGIN {
    eval { require Text::CSV_XS; Text::CSV_XS->import(); $Have_CSV_XS = 1; 1 } or do {
        require Text::CSV; Text::CSV->import();
    };
}

##############################################################################
# Command-line interface
# Required CSV headers (case-sensitive):
#   OD_INSTAME_SRC, AGID_NAME_SRC, OD_INSTAME_DST, AGID_NAME_DST, OD_TSM_LOGON_NAME
##############################################################################
my $CSV_FILE   = "";
my $SERVERNAME = "";
my $USER       = "";
my $PASS       = "";
my $PWFILE     = "";
my $DELIM      = ",";   # CSV delimiter, e.g. ';' for semicolon CSV
my $DO_RUN     = 0;     # default = dry-run (no changes)
my $VERIFY     = 0;     # optional post-rename verification (only with --run)

while (my $arg = shift @ARGV) {
    if    ($arg eq '-c' or $arg eq '--csv')        { $CSV_FILE = shift(@ARGV) // "" }
    elsif ($arg eq '--servername')                 { $SERVERNAME = shift(@ARGV) // "" }
    elsif ($arg eq '-u' or $arg eq '--user')       { $USER = shift(@ARGV) // "" }
    elsif ($arg eq '-p' or $arg eq '--pass')       { $PASS = shift(@ARGV) // "" }
    elsif ($arg eq '--pwfile')                     { $PWFILE = shift(@ARGV) // "" }
    elsif ($arg eq '--delim')                      { $DELIM = shift(@ARGV) // "," }
    elsif ($arg eq '--run')                        { $DO_RUN = 1 }
    elsif ($arg eq '--verify')                     { $VERIFY = 1 }
    elsif ($arg eq '-h' or $arg eq '--help')       { print_usage(); exit 0 }
    else { die "Unknown arg: $arg (use --help)\n" }
}

die "ERROR: CSV file required (-c|--csv)\n" unless $CSV_FILE;
-f $CSV_FILE or die "ERROR: CSV not readable: $CSV_FILE\n";

# Password resolution:
# - If --pwfile is provided, read the password from that file.
# - If --run and we have a user but no password yet, prompt securely.
if ($PWFILE && !$PASS) {
    open my $pfh, "<:encoding(UTF-8)", $PWFILE or die "ERROR: Cannot read --pwfile $PWFILE: $!\n";
    chomp($PASS = <$pfh> // "");
    close $pfh;
}
if ($DO_RUN && $USER && !$PASS) {
    print STDERR "Enter password for '$USER': ";
    system("stty -echo"); chomp($PASS = <STDIN> // ""); system("stty echo"); print STDERR "\n";
}

##############################################################################
# Helper functions
##############################################################################
sub nowstamp { return strftime("%Y%m%d_%H%M%S", localtime); }

# Quote strings for dsmadmc: use double quotes, escape internal quotes by doubling them.
sub q_tsm    { my ($s)=@_; $s =~ s/"/""/g; return qq{"$s"}; }

# Build the dsmadmc command with our standard flags:
# -dataonly/-comma for machine-readable output, -noconfirm to avoid Y/N prompts.
sub build_dsmadmc_cmd {
    my ($tsm_cmd) = @_;
    my @cmd = ("dsmadmc", "-dataonly=yes", "-comma", "-noconfirm");
    push @cmd, "-servername=$SERVERNAME" if $SERVERNAME;
    push @cmd, "-id=$USER"               if $USER;
    push @cmd, "-password=$PASS"         if $PASS ne '';
    push @cmd, $tsm_cmd;
    return @cmd;
}

# Run dsmadmc and capture both stdout and stderr.
sub run_dsmadmc {
    my ($tsm_cmd) = @_;
    my @cmd = build_dsmadmc_cmd($tsm_cmd);
    my $err = gensym;
    my $pid = open3(undef, \my $out, $err, @cmd);
    local $/ = undef;
    my $stdout = <$out> // "";
    my $stderr = <$err> // "";
    waitpid($pid, 0);
    my $rc = $? >> 8;
    return ($rc, $stdout, $stderr);
}

# Query filespaces for a node and return the list of FILESPACE_NAME values.
# dsmadmc with -comma prints CSV rows; FILESPACE_NAME is the 2nd column.
sub fs_list_for_node {
    my ($node) = @_;
    my ($rc, $out, $err) = run_dsmadmc("q filespace $node");
    return () if $rc != 0;
    my @names;
    my $csv = ($Have_CSV_XS ? Text::CSV_XS->new({binary=>1}) : Text::CSV->new({binary=>1}));
    for my $line (split /\r?\n/, $out) {
        next unless length $line;
        if ($csv->parse($line)) {
            my @fields = $csv->fields();
            next unless @fields >= 2;
            push @names, $fields[1];
        }
    }
    return @names;
}

# Convenience check: does a given filespace exist for this node?
sub fs_exists {
    my ($node, $fs) = @_;
    my @lst = fs_list_for_node($node);
    for my $x (@lst) { return 1 if defined $x && $x eq $fs }
    return 0;
}

##############################################################################
# Output files: report CSV and rollback .sh
##############################################################################
my $stamp       = nowstamp();
my $REPORT_PATH = "fs_rename_report_${stamp}.csv";
my $ROLLBACK_SH = "fs_rollback_${stamp}.sh";

open my $rep, ">:encoding(UTF-8)", $REPORT_PATH or die "ERROR: Cannot write $REPORT_PATH: $!\n";
print $rep "node,old_fs,new_fs,action,status,message\n";
close $rep;

open my $rb, ">:encoding(UTF-8)", $ROLLBACK_SH or die "ERROR: Cannot write $ROLLBACK_SH: $!\n";
print $rb "#!/usr/bin/env bash\n";
print $rb "# Rollback script generated by cmod_fs_rename.pl\n";
print $rb "# Each line renames a filespace back to its original value\n\n";
close $rb;
chmod 0755, $ROLLBACK_SH;

##############################################################################
# CSV input
# Notes on Windows CSV (CRLF): Text::CSV(_XS) handles CRLF automatically.
# We still trim trailing CRs and spaces from field values in _clean().
##############################################################################
my $csv = ($Have_CSV_XS ? Text::CSV_XS->new({ binary=>1, sep_char=>$DELIM, auto_diag=>1 })
                        : Text::CSV->new({  binary=>1, sep_char=>$DELIM, auto_diag=>1 }));

open my $fh, "<:encoding(UTF-8)", $CSV_FILE or die "ERROR: Cannot read $CSV_FILE: $!\n";

# Read header – we rely on exact header names (as delivered in your file)
my $header = $csv->getline($fh) or die "ERROR: Empty CSV or cannot read header\n";
$csv->column_names(@$header);

# Required headers present?
my @need = qw(OD_INSTAME_SRC AGID_NAME_SRC OD_INSTAME_DST AGID_NAME_DST OD_TSM_LOGON_NAME);
for my $h (@need) {
    my $ok = grep { defined $_ && $_ eq $h } @$header;
    die "ERROR: CSV missing required header: $h\n" unless $ok;
}

##############################################################################
# Main loop: read each row and construct/execute rename filespace logic
##############################################################################
while (my $row = $csv->getline_hr($fh)) {

    # Key business rule from CMOD side:
    # - if OD_TSM_LOGON_NAME is empty -> CMOD used only CACHE (no archiving to SP)
    #   => skip the row but log it into the report.
    my $node     = _clean($row->{OD_TSM_LOGON_NAME} // "");
    if ($node eq "") {
        append_report("", "", "", "skip", "SKIPPED", "OD_TSM_LOGON_NAME empty");
        next;
    }

    # Build old/new filespace names from the 4 columns:
    my $src_inst = _clean($row->{OD_INSTAME_SRC} // "");
    my $src_agid = _clean($row->{AGID_NAME_SRC}  // "");
    my $dst_inst = _clean($row->{OD_INSTAME_DST} // "");
    my $dst_agid = _clean($row->{AGID_NAME_DST}  // "");

    my $old_fs = "/" . $src_inst . "/" . $src_agid;
    my $new_fs = "/" . $dst_inst . "/" . $dst_agid;

    my $tsm_cmd = "rename filespace $node " . q_tsm($old_fs) . " " . q_tsm($new_fs);

    # DRY-RUN (default): print planned command, record DRY-RUN in the report.
    if (!$DO_RUN) {
        print STDERR "INFO : [DRY] $tsm_cmd\n";
        append_report($node, $old_fs, $new_fs, "rename", "DRY-RUN", "No changes applied");
        next;
    }

    # --run path: do safety checks before renaming
    unless (fs_exists($node, $old_fs)) {
        append_report($node, $old_fs, $new_fs, "rename", "SKIPPED", "old_fs not found");
        next;
    }
    if (fs_exists($node, $new_fs)) {
        append_report($node, $old_fs, $new_fs, "rename", "SKIPPED", "new_fs already exists");
        next;
    }

    # Execute rename filespace via dsmadmc
    print STDERR "INFO : Executing: $tsm_cmd\n";
    my ($rc, $out, $err) = run_dsmadmc($tsm_cmd);
    if ($rc != 0) {
        my $msg = $err || $out || "";
        $msg =~ s/[\r\n,]/ /g; # keep report CSV readable
        append_report($node, $old_fs, $new_fs, "rename", "FAILED", $msg);
        next;
    }

    my $status = "OK";
    my $msg    = "renamed";

    # Optional verification: query filespaces again and check that
    #  - new_fs exists, and old_fs no longer exists for this node.
    if ($VERIFY) {
        my $has_new = fs_exists($node, $new_fs);
        my $has_old = fs_exists($node, $old_fs);
        if ($has_new && !$has_old) {
            $msg .= "; verified";
        } else {
            $status = "WARN";
            $msg   .= "; verification mismatch";
        }
    }

    append_report($node, $old_fs, $new_fs, "rename", $status, $msg);

    # Rollback entry (only if a rename was performed successfully)
    if ($status eq "OK" || $status eq "WARN") {
        append_rollback("rename filespace $node " . q_tsm($new_fs) . " " . q_tsm($old_fs));
    }
}

close $fh;

print STDERR "INFO : Done. Report: $REPORT_PATH\n";
print STDERR "INFO : Rollback script: $ROLLBACK_SH\n" if $DO_RUN;

##############################################################################
# Support subs
##############################################################################
sub _clean {
    my ($v) = @_;
    return "" unless defined $v;
    $v =~ s/\r$//;        # strip trailing CR (Windows CRLF case)
    $v =~ s/^\s+|\s+$//g; # trim spaces
    return $v;
}

# Append a single line to the CSV report. We CSV-escape fields when needed.
sub append_report {
    my ($node,$old,$new,$action,$status,$msg) = @_;
    open my $rfh, ">>:encoding(UTF-8)", $REPORT_PATH or die "Cannot append $REPORT_PATH: $!";
    print $rfh join(",", map { _csv_safe($_) } ($node,$old,$new,$action,$status,$msg)), "\n";
    close $rfh;
}

# Append one rollback command line to the .sh file.
sub append_rollback {
    my ($line) = @_;
    open my $rbfh, ">>:encoding(UTF-8)", $ROLLBACK_SH or die "Cannot append $ROLLBACK_SH: $!";
    print $rbfh "$line\n";
    close $rbfh;
}

# Minimal CSV escaping for the report file.
sub _csv_safe {
    my ($s) = @_;
    $s = "" unless defined $s;
    if ($s =~ /[",\r\n]/) {
        $s =~ s/"/""/g;
        return qq{"$s"};
    }
    return $s;
}

sub print_usage {
    print <<'USAGE';
Usage:
  cmod_fs_rename.pl -c <file.csv> [--servername STANZA] [-u USER] [-p PASS | --pwfile FILE]
                    [--delim ';'] [--run] [--verify]

What it does:
  - Reads mapping from CSV:
      node_name = OD_TSM_LOGON_NAME
      old_fs    = /OD_INSTAME_SRC/AGID_NAME_SRC
      new_fs    = /OD_INSTAME_DST/AGID_NAME_DST
  - Skips rows with empty OD_TSM_LOGON_NAME (CMOD used only CACHE) and logs them.
  - Default is DRY-RUN (prints planned 'rename filespace' commands, no changes).
  - With --run:
      * Pre-check: old_fs must exist for node; new_fs must NOT exist.
      * Executes:  rename filespace <node> "<old_fs>" "<new_fs>"
      * --verify: re-queries filespaces to confirm the change.
  - Writes a CSV report: fs_rename_report_YYYYmmdd_HHMMSS.csv
  - Writes a rollback script (when --run): fs_rollback_YYYYmmdd_HHMMSS.sh
    Each line reverts one rename:
      rename filespace <node> "<new_fs>" "<old_fs>"

dsmadmc flags used:
  -dataonly=yes -comma : machine-friendly output for parsing
  -noconfirm           : suppress any Y/N prompts

Required CSV headers (exact names):
  OD_INSTAME_SRC, AGID_NAME_SRC, OD_INSTAME_DST, AGID_NAME_DST, OD_TSM_LOGON_NAME
USAGE
}