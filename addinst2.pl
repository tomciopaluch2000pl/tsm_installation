#!/usr/bin/perl
#
# NAME:
#   add_instance_2_tsm_client.pl
#
# DESCRIPTION:
#   This script automates the creation of a new TSM client instance on AIX 7.2.
#   It creates the required instance directory and configuration files, appends
#   a server stanza to existing dsm.sys files, and ensures proper ownership and
#   permissions for odadm:odadmgrp.
#
# USAGE:
#   # Dry-run (default, no changes made):
#   perl add_instance_2_tsm_client.pl \
#        --servername ODLAHU01 \
#        --tcpserveraddress a333-7102-4498.ubsglobal-prod.msad.ubs.net \
#        --tcpport 3010
#
#   # Real execution (changes applied):
#   perl add_instance_2_tsm_client.pl \
#        --servername ODLAHU01 \
#        --tcpserveraddress a333-7102-4498.ubsglobal-prod.msad.ubs.net \
#        --tcpport 3010 --run
#
# OPTIONS:
#   --servername        Instance/server name (e.g., ODLAHU01).
#   --tcpserveraddress  TCP server address (FQDN or IP).
#   --tcpport           TCP port number.
#   --run               Execute changes (default is dry-run).
#   --force             Overwrite instance files if they already exist.
#   --help              Print usage instructions.
#
# NOTES:
#   - Without --run, the script performs a dry-run (simulation only).
#   - Must be executed as root if --run is specified.
#   - System dsm.sys files (/usr/tivoli/tsm/client/ba/bin/dsm.sys and
#     /usr/tivoli/tsm/client/api/bin64/dsm.sys) are never overwritten,
#     only appended (with backup).
#   - Instance-specific dsm.opt and dsm.sys files are created under:
#       /opt/tivoli/tsm/instance/<servername>
#     Use --force if overwriting is required.
#
# AUTHOR:
#   [Your Team / Your Name]
#
# PLATFORM:
#   AIX 7.2
#
# LAST UPDATE:
#   2025-08-29
#
# ---------------------------------------------------------------------------

use strict;
use warnings;
use Getopt::Long qw(GetOptions);
use File::Path qw(make_path);
use POSIX qw(strftime);

# -------- CLI --------
my ($servername, $tcpserveraddress, $tcpport, $run, $force, $help);
GetOptions(
    'servername=s'      => \$servername,
    'tcpserveraddress=s'=> \$tcpserveraddress,
    'tcpport=i'         => \$tcpport,
    'run!'              => \$run,
    'force!'            => \$force,
    'help!'             => \$help,
) or die "Invalid options. Use --help.\n";

if ($help || !$servername || !$tcpserveraddress || !$tcpport) {
    print <<"USAGE";
Usage:
  perl add_instance_2_tsm_client.pl \\
       --servername ODLAHU01 \\
       --tcpserveraddress a333-7102-4498.ubsglobal-prod.msad.ubs.net \\
       --tcpport 3010 [--run] [--force]

Examples:
  # Dry-run (default)
  perl add_instance_2_tsm_client.pl --servername ODLAHU01 --tcpserveraddress host --tcpport 3010

  # Real execution
  perl add_instance_2_tsm_client.pl --servername ODLAHU01 --tcpserveraddress host --tcpport 3010 --run

Options:
  --run       Execute changes (default is dry-run).
  --force     Overwrite instance files if they already exist.
  --help      Show this help.
USAGE
    exit 0;
}

# If --run is not set → dry-run mode
my $dry_run = !$run;

# Must be root (only when running real changes)
if (!$dry_run && $> != 0) {
    die "[ERR ] Please run as root when using --run.\n";
}

# -------- Constants --------
my $owner_user  = 'odadm';
my $owner_group = 'odadmgrp';

my $inst_base   = '/opt/tivoli/tsm/instance';
my $inst_dir    = "$inst_base/$servername";

my $ba_sys      = '/usr/tivoli/tsm/client/ba/bin/dsm.sys';
my $api_sys     = '/usr/tivoli/tsm/client/api/bin64/dsm.sys';

# Content blocks
my $dsm_opt_content = <<"OPT";
SERVERNAME $servername
QUIET
OPT

my $dsm_sys_block = <<"SYS";
*Federated SP Server 4 LA-HUB
Servername $servername
COMMMethod tcpip
TCPServeraddress $tcpserveraddress
TCPPort $tcpport
ENABLEARCHIVERETENTIONPROTECTION yes
SYS

# -------- Helpers --------
sub ts { strftime("%Y%m%d-%H%M%S", localtime) }

sub info { print shift }

sub ensure_dir {
    my ($dir) = @_;
    if (-d $dir) { info "[INFO] Directory exists: $dir\n"; return; }
    if ($dry_run) { info "[DRY ] Would create directory: $dir\n"; return; }
    make_path($dir, { mode => 0755 }) or die "[ERR ] Cannot create $dir: $!\n";
    info "[OK  ] Created directory: $dir\n";
}

sub write_file_exact {
    my ($path, $content, $mode) = @_;
    if (-e $path && !$force) {
        die "[ERR ] $path already exists. Re-run with --force to overwrite.\n";
    }
    if ($dry_run) { 
        info "[DRY ] Would write file: $path (mode ".sprintf("%04o",$mode).")\n"; 
        return; 
    }
    open my $fh, '>', $path or die "[ERR ] Cannot write $path: $!\n";
    print {$fh} $content or die "[ERR ] Failed writing to $path: $!\n";
    close $fh or die "[ERR ] Failed closing $path: $!\n";
    chmod($mode, $path) or die "[ERR ] chmod ".sprintf("%04o",$mode)." for $path failed: $!\n";
    info "[OK  ] Written: $path\n";
}

sub chown_tree {
    my ($path, $user, $group) = @_;
    my $uid = (getpwnam($user))[2];
    my $gid = (getgrnam($group))[2];
    die "[ERR ] User $user not found\n"  unless defined $uid;
    die "[ERR ] Group $group not found\n" unless defined $gid;

    if ($dry_run) { info "[DRY ] Would chown -R $user:$group $path\n"; return; }

    require File::Find;
    my $count = 0;
    File::Find::find({
        wanted => sub { chown $uid, $gid, $File::Find::name and $count++; },
        no_chdir => 1
    }, $path);
    info "[OK  ] chown -R $user:$group $path (items: $count)\n";
}

sub file_contains_block {
    my ($path, $block) = @_;
    return 0 unless -e $path;
    open my $fh, '<', $path or die "[ERR ] Cannot read $path: $!\n";
    local $/ = undef;
    my $all = <$fh>;
    close $fh;
    return index($all, $block) != -1;
}

sub backup_file {
    my ($path) = @_;
    return unless -e $path;
    my $bak = "$path.bak.".ts();
    if ($dry_run) { info "[DRY ] Would create backup: $bak\n"; return; }

    link($path, $bak) || do {
        open my $in,  '<', $path or die "[ERR ] Cannot read $path: $!\n";
        open my $out, '>', $bak  or die "[ERR ] Cannot write $bak: $!\n";
        binmode $in; binmode $out;
        my $buf;
        while (read($in, $buf, 8192)) { print $out $buf or die $!; }
        close $in; close $out;
    };
    info "[OK  ] Backup: $bak\n";
}

sub append_block_if_missing {
    my ($path, $block) = @_;
    die "[ERR ] Missing file: $path (install TSM client first).\n" unless -e $path;

    if (file_contains_block($path, $block)) {
        info "[INFO] Target block already present in $path — skipping.\n";
        return;
    }

    backup_file($path);

    if ($dry_run) { 
        info "[DRY ] Would append block to: $path\n";
        return;
    }
    open my $fh, '>>', $path or die "[ERR ] Cannot append to $path: $!\n";
    print {$fh} "\n$block\n" or die "[ERR ] Append failed for $path: $!\n";
    close $fh;
    info "[OK  ] Appended block to: $path\n";
}

sub ensure_group_rw_for_user {
    my ($path, $user, $group) = @_;
    my $gid = (getgrnam($group))[2];
    die "[ERR ] Group $group not found\n" unless defined $gid;

    my @st = stat($path) or die "[ERR ] Cannot stat $path: $!\n";
    my $mode = $st[2] & 07777;
    my $cur_gid = $st[5];

    if ($cur_gid != $gid) {
        if ($dry_run) { info "[DRY ] Would chgrp $group $path\n"; }
        else { chown -1, $gid, $path or die "[ERR ] chgrp $group $path failed: $!\n"; info "[OK  ] Set group $group on $path\n"; }
    } else {
        info "[INFO] Group already $group for $path\n";
    }

    if (($mode & 0060) != 0060) { # g+rw
        my $newmode = $mode | 0060;
        if ($dry_run) { 
            printf "[DRY ] Would chmod %04o -> %04o on %s (g+rw)\n", $mode, $newmode, $path;
        } else {
            chmod $newmode, $path or die "[ERR ] chmod g+rw $path failed: $!\n";
            printf "[OK  ] Permissions %04o -> %04o on %s\n", $mode, $newmode, $path;
        }
    } else {
        printf "[INFO] Group permissions already sufficient on %s (mode=%04o)\n", $path, $mode;
    }
}

# -------- Main steps --------

ensure_dir($inst_dir);

write_file_exact("$inst_dir/dsm.opt", $dsm_opt_content, 0644);
write_file_exact("$inst_dir/dsm.sys",  $dsm_sys_block,  0644);

chown_tree($inst_base, $owner_user, $owner_group);

append_block_if_missing($ba_sys,  $dsm_sys_block);
append_block_if_missing($api_sys, $dsm_sys_block);

ensure_group_rw_for_user($ba_sys,  $owner_user, $owner_group);
ensure_group_rw_for_user($api_sys, $owner_user, $owner_group);

info "[DONE] All steps completed".($dry_run ? " (dry-run, no changes made)" : "").".\n";