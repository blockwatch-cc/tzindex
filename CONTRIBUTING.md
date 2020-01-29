# Contributing to Tzindex

Thanks for your interest in contributing to TzIndex! This document outlines some of the conventions on the development workflow, commit message formatting, contact points and other resources.

If you need any help or mentoring getting started, understanding the codebase, or making a PR (or anything else really), please ask on [Discord](https://discord.gg/D5e98Hw).


## Building and setting up a development workspace

TzIndex is written in Golang and uses Go modules, so it should work with Go versions >= 1.11.

### Prerequisites

To build TzIndex you'll need to at least have the following installed:

* `git` - Version control
* `go` - Golang compiler and standard library
* `make` - Build tool (run common workflows, optional)

### Getting the repository

```
git clone https://github.com/blockwatch-cc/tzindex.git
cd tzindex
# Future instructions assume you are in this directory
```

### Building and testing

TzIndex includes a `Makefile` with common workflows, you can also use `go build` or `go run`, as you would in other Golang projects.

You can build TzIndex:

```bash
make build
```

A common test suite is not included with the project, but you may implement your own tests and run

```bash
go test
```

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

- Create a Git branch from where you want to base your work. This is usually master.
- Write code, add test cases (optional right now), and commit your work (see below for message format).
- Run tests and make sure all tests pass (optional right now).
- Push your changes to a branch in your fork of the repository and submit a pull request.
- Your PR will be reviewed by a maintainer, who may request some changes.
  * Once you've made changes, your PR must be re-reviewed and approved.
  * If the PR becomes out of date, you can use GitHub's 'update branch' button.
  * If there are conflicts, you can rebase (or merge) and resolve them locally. Then force push to your PR branch.
    You do not need to get re-review just for resolving conflicts, but you should request re-review if there are significant changes.
- A maintainer will test and merge your pull request.

Thanks for your contributions!

### Format of the commit message

We follow a rough convention for commit messages that is designed to answer two
questions: what changed and why. The subject line should feature the what and
the body of the commit should describe the why.

```
etl/model: changed how block rewards are counted

Blocks were counting all rewards and deposits including endorsements,
which was confusing and unclear what part was going to the block baker.
This update changes the rewards and deposits fields to only count
amounts related to baking.
```

The format can be described more formally as follows:

```
<subsystem>: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
Signed-off-by: <Name> <email address>
```

The first line is the subject and should be no longer than 50 characters, the other lines should be wrapped at 72 characters (see [this blog post](https://preslav.me/2015/02/21/what-s-with-the-50-72-rule/) for why).

If the change affects more than one subsystem, you can use comma to separate them like `etl/model,etl/index:`.

If the change affects many subsystems, you can use ```*``` instead, like ```*:```.

The body of the commit message should describe why the change was made and at a high level, how the code works.

### Signing off the Commit

The project uses [DCO check](https://github.com/probot/dco#how-it-works) and the commit message must contain a `Signed-off-by` line for [Developer Certificate of Origin](https://developercertificate.org/).

Use option `git commit -s` to sign off your commits.
