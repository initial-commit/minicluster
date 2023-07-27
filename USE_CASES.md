Use Cases
=========

Minicluster can be used in various ways, especially when used not through its
commands only, but also as a library in your own scripts.

Minicluster is currently in its infancy, since there is not even a 0.x release
yet. But in my opinion, the more use-cases are developed prior to 1.0.0, the
more robust and reusable minicluster is going to be.

This being said, this is the list of use cases that I would like to prototype.

Official packages
-----------------

These use cases are around the oficial, compiled and packages archlinux
packages available through pacman, e.g. core, extra, multilib, etc.

* download all packages and build the sqlite db for all of them, without keeping the packages around
* on top of it: find all packages which include a `__pycache__` directory


AUR packages
------------

These use cases revolve around the AUR: building custom repositories of
compiled packages, verifying, safe upgrading of the machines in your network,
testing in VMs before upgrading real systems.

* download and verify source without unpacking, warn packages on AUR about the problems
* build package
* scan built packages for viruses
* make a package repository from a predefined list of packages
* crawl for outdated packages from each package's upstream


Around itself
-------------

These use-cases revolve around minicluster itself

* continuously build itself, check for problems, and if no problems, update itself
* almost unattended installation via netboot/tftp


Applications
------------

These use-cases are about various, well-known software, and packaging them in
reusable VMs. The corresponding VMs will provide the regular commands, as well
as commands tailored to the specifics of each software.

Example commands:

* upgrade
* downgrade
* fail-over
* check-health
* backup
* restore
* monitor
* cleanup
* test

All applications are required to provide all applicable commands, as well as
choices of a selection of logging strategies: syslog or distributed tracing.

Selection of applications:

* haproxy + certbot
* nginx
* syslog-ng
* postgresql
* etcd
* nifi
* redis
* glusterfs
* proftpd
* buildbot
* ticketing system: TBD
* moodle
* postfix + dovecot
* plausible
* nextcloud

Of course, you can develop your own appliances for your favourite tools as
well. The list showcases the technologies on my radar for a start.


The applications are required to provide a guided code generation, and the code
is using minicluster as a library whenever possible to achieve its goals.


Custom Applications
-------------------

The goal of these use-cases is to provide templates for the most up-to-date
frameworks or technologies in combination.

Example tech stack and architecture:

* java spring + a webcomponents framework (subject to change), hexagonal
  architecture, vertically sliced monolith, highly available with 2 proxies
  (haproxy), logical replication (postgresql), 3 worker servers; the
  architecture has a bunch of guardrails built-in, both purely technical as
  also organizatorial. Goal: you get started right away with your domain
  modelling. Supports also other main jvm languages: kotlin, scala3, groovy. Of
  course, all done with TDD.
* same architectural traits as above, but for the Python ecosystem
* same, but for PHP

For the frontend, generally target typescript, but do not shy away from
dart/flutter


Personal
--------

These use-cases are about how I use minicluster

* my homelab: minicluster itself, various use-cases for aur and arch
  repositories/community, nextcloud
* my website: getnikola among others
* the initial-commit website


I will consider minicluster to be stable enough for releasing 1.0.0 when my
personal use cases have been running with stability for many months.


Commercial applications
-----------------------

Any company is welcome to build their infrastructure around minicluster.

I have certain ideas for my own applications:

* skillmatrix
* tasker
