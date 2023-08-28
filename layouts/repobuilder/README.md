Motivation
==========

This use-case of minicluster showcases how to build a simple minicluster.

While it is useful only for a small audience, the building blocks it provides
can be utilized to build a larger ecosystem on top of archlinux.

Audience:

* archlinux regular users
* archlinux advanced users
* archlinux AUR package maintainers
* archlinux system administrators
* homelab users using archlinux

Problems it solves:

* detecting the mistakes of maintainers in AUR package `PKGBUILD`s
* reporting errors in these packages
* detecting updates in packages following well-known versioning heuristics
  (e.g. semver) with the goal of marking packages as out-of-date in aur
* putting together archlinux package repositories (akin to `core` or `extra`),
  including their transitive dependencies
* re-compiling AUR packages when some of their build dependencies get updates
* serving via https or other protocols your own repositories

<!---
Hi. I'm half happy with my results so far with checking aur packages. I've been working in the last days and I got the tools    
necessary to check PKGBUILDs more thoroughly. One run should take by current estimations about 4.5h, so I got time to chat until
the next uncaught exception happens                                                                                             

So if anyone happens to want to read and give their ideas, I would appreciate it                                                

Basically I'm working on this project https://github.com/initial-commit/minicluster                                             

It is revolving around arch, builds qcow2 images and repositories inside nested VMs (as a safety net to make sure the artefacts 
are really self-contained) - it doesn't matter so much for this channel, but maybe interesting to know                          
artifacts are qcow2 images, archlinux repositories (think repo-add)                                                             

now to the part related to aur:                                                                                                 

one of the first use cases I want to tackle is quality of AUR because I think it's easy enough to do and would give me some     
feedback and also be useful to someone early on during the development                                                          
so I got in a sqlite db quite some information about packages from core, extra and aur, including dependencies which will help  
me build the dependency graph                                                                                                   

what I'm currently running is basically printsrcinfo, but inside such a VM                                                      

there are quite some errors in pkgbuilds and I have the feeling their number will grow                                          

invalid utf-8 characters for instance                                                                                           
.SRCINFO not updated                                                                                                            
undeclared dependencies                                                                                                         
and so on, I think a lot can be caught this way                                                                                 

whatever errors happen during printsrcinfo are going to be logged also to the database                                          

and within the scope of this use case, I want to additionally check the URLs for existence                                      
from the SOURCEs                                                                                                                

interesting would be also to attempt by some heuristics also to download what could be the next version for packages which seem 
to follow semver                                                                                                                
e.g. current pkgver 0.0.1, possible next: 0.0.2, 0.1.0, 1.0.0                                                                   
from what I saw in aurs gitlab tickets, there's a need for a flag for all packages to notify the author that "something is wrong
with your package"                                                                                                              
beside "out of date"                                                                                                            
for that, I will suggest to make a drop down/selection of reasons for what can be wrong with a package                          
--->

INSTALLATION
============


USAGE
=====


Roadmap
=======

This is the roadmap for this use-case.

The concrete versions have not been defined, as the planning for this
particular use-case must fit the overall roadmap of minicluster and all other
use-cases used as a foundation for minicluster 1.0.0.

v0.0.1
------

Goal: programmatic usage and helping out AUR maintainers via AUR comments

v0.0.2
------

Goal: self-contained build and continuous operation

* monitoring of the service

v0.x.0
------

Goal: construct/define and recompile own repositories

v0.x.0
------

Goal: multi-user UI and repositories

v0.x.0
------

Goal: generally available to ArchLinux users


v0.x.0
------

Goal: re-implement with a new DSL
