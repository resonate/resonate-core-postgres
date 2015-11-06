# Resonate Core PostgreSQL

Resonate Core PostgreSQL is a collection of libraries and a build system written at Resonate to reliably develop, maintain, and deploy multi-terabyte PostgreSQL database clusters.

## Quick Start

The sample is designed to run as a Vagrant VM.  You'll need to install Vagrant and VirtualBox to get started.  Once they are installed change to the repo directory and run:
```
vagrant up
vagrant ssh
cd /resonate/build
```

Now we'll perform a full build by running the following:
```
ant execute.build
```
That's it - now there is a database called sample_dev with some data in sample.sample_table.

It's possible to rebuild the database:
```
ant execute.rebuild
```
Be careful as this will drop the old database before creating the new one.

Updating a database is a complex topic, but here's a simple example:
```
ant -Drelease=release2 -Drelease.update=release1 execute.update
```
This update simply adds more data to sample.sample_table but much more complicated updates are possible.
