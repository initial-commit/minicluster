Introduction
============

Minicluster is a library for setting up infrastructure.

It can be used in various ways:

* as a platform-engineering tool for developers
* for black-box testing and system testing (also in failure scenarios)
* to develop cloud-based Linux images
* for setting up and operating homelabs
* for safe deployment to production of provably working systems
* for testing dedicated hardware by giving control over the kernel and kernel
  drivers
* as the name suggests, all of the above in a cluster, including features like
  failover, backup, restore, observability (logging, monitoring, health-checks)
  and more

By combining these aspects within the same organization, it can cover the full
needs, from development to deploying software into production.

Minicluster is optimized for

* developer experience
* gaining knowledge
* robust, reproducible environments
* promoting the infrastructure to production with peace of mind
* observability

Minicluster has commands to do basic things. However, to model complex
pipelines/processes, you'll have to write python code.

Advantages
==========

WIP

* it's a library, so you write just code in python (more languages once the API is stable)
* it fosters knowledge by tackling the problems at the most fundamental level,
  instead of building abstractions on top (it doesn't use yaml or the like)

Disadvantages
=============

WIP

Example Use-Cases
=================

The project is currently in the bootstrapping phase, examples will be added here.

License
=======

Minicluster is licensed under the GNU AGPLv3, see LICENSE.txt for details.

For exceptions and/or business oportunities, contact me at 
`Flavius Aspra <flavius.as+minicluster@gmail.com>`.

```
    Minicluster - craft your clusters easily and reliably.
    Copyright (C) 2003  Flavius-Adrian ASPRA <flavius.as+minicluster@gmail.com>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
```
