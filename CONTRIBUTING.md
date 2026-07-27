# Contributing to Nexus N3 Core

Thank you for your interest in contributing to Nexus N3 Core.

Nexus N3 Core is the locally deployed orchestration and execution platform at the centre of the Nexus N3 ecosystem. Contributions that improve reliability, hardware support, documentation, testing and developer experience are welcome.

This repository has contribution and licensing requirements that differ from other repositories in the Nexus-N3 organisation. Please read this document before opening a pull request.

## Licence

Nexus N3 Core is distributed under the:

**GNU General Public License v3.0 only (`GPL-3.0-only`)**

By submitting a contribution, you agree that accepted contributions may be included in releases of Nexus N3 Core under this licence and in accordance with the applicable Contributor Licence Agreement.

Other Nexus N3 repositories, including plugins, SDKs and tooling, may use the MIT License and may have different contribution requirements.

Contributor Licence Agreement

Before an external contribution can be accepted, the contributor must agree to the current Rightstep OÜ Contributor Licence Agreement.

The agreement does not transfer ownership of your contribution to Rightstep OÜ. You retain ownership of your work while granting Rightstep OÜ the rights required to use, modify, distribute, sublicense and license the contribution.

Individual contributions

Individuals must complete the:

Individual Contributor Licence Agreement

Send the completed and signed agreement to mike@rightstep-health.com.

Signed agreements are retained privately by Rightstep OÜ and are not committed to the public repository.

Contributions owned by an employer or organisation

Do not sign the Individual Contributor Licence Agreement for code that you do not personally own.

If your employer or another legal entity owns your contribution, an authorised representative of that organisation may need to complete the:

Entity Contributor Licence Agreement

Contact mike@rightstep-health.com before submitting company-owned contributions.

Rightstep OÜ contributors

Contributions made by Rightstep OÜ personnel or contractors may be covered by separate employment, contractor or intellectual-property agreements and may not require the public ICLA process.

## Before contributing

Before starting substantial work:

1. Search the existing issues and pull requests.
2. Open an issue describing the proposed change.
3. Explain the problem, intended behaviour and expected scope.
4. Wait for maintainer feedback before implementing a large architectural change.

Small corrections, documentation fixes and clearly isolated bug fixes may be submitted directly.

## Development setup

Follow the development and installation instructions in the repository `README.md`.

Create a dedicated branch for your work:

```bash
git switch -c feature/short-description
```

Use a clear branch prefix where appropriate:

```text
feature/
fix/
docs/
refactor/
test/
```

Keep each branch focused on one logical change.

## Code changes

Contributions should:

* follow the existing project structure and coding conventions;
* preserve compatibility with supported deployment environments;
* include appropriate error handling and logging;
* avoid introducing unnecessary dependencies;
* avoid coupling plugins directly to private core implementation details;
* maintain documented process, RPC and plugin-runtime boundaries;
* include type annotations where consistent with the surrounding code;
* update documentation when behaviour or configuration changes.

Do not include unrelated formatting changes or large refactors in a focused bug-fix pull request.

## Tests

New behaviour should include tests where practical.

Before opening a pull request, run the relevant test, linting and formatting commands documented by the repository.

At minimum, confirm that:

* existing tests still pass;
* new functionality has appropriate coverage;
* configuration examples remain valid;
* startup and shutdown paths are not broken;
* plugin and runtime boundaries remain compatible;
* changes do not introduce platform-specific assumptions without documentation.

Describe any testing that could not be performed in the pull request.

## Third-party code and dependencies

Only submit work that you have the authority to contribute.

Do not copy source code, documentation, images or other material from another project unless:

* its licence permits inclusion;
* the original source is identified;
* the licence is compatible with this repository;
* all required notices and attribution are included.

Adding a dependency is not the same as copying its source into this repository, but new dependencies must still be reviewed for licence compatibility, maintenance status and security impact.

Identify all new dependencies and third-party material in the pull request description.

## Commit messages

Use short, descriptive commit messages written in the imperative form.

Examples:

```text
Add plugin startup timeout handling
Fix duplicate sensor discovery events
Document local development setup
```

Keep commits logically organised. Avoid committing generated files, local configuration, credentials, virtual environments or build artefacts.

## Pull requests

A pull request should include:

* a clear description of the problem;
* an explanation of the proposed solution;
* details of the testing performed;
* relevant issue references;
* documentation updates where required;
* disclosure of new dependencies or third-party material;
* any compatibility or migration considerations.

Draft pull requests are welcome for early technical review.

Before a pull request can be merged:

* any required Individual or Entity Contributor Licence Agreement must have been completed and accepted by Rightstep OÜ;
* automated tests and required checks must pass;
* maintainer review must be completed;
* review comments must be resolved;
* the branch must be up to date where required.

Rightstep OÜ may decline contributions that conflict with the project architecture, product direction, security requirements or maintenance capacity.

## Pull request declaration

By submitting a pull request, you confirm that:

* you created the contribution or have authority to submit it;
* you have complied with any applicable employer intellectual-property policies;
* you have identified any included third-party material;
* you understand that the contribution will be subject to the applicable Contributor Licence Agreement;
* you understand that accepted contributions may be distributed as part of Nexus N3 Core.

## Security issues

Do not publicly disclose a suspected security vulnerability through a normal GitHub issue.

Report security concerns privately to:

[mike@rightstep-health.com](mailto:mike@rightstep-health.com)

Include:

* the affected component;
* steps to reproduce the issue;
* the potential impact;
* any suggested mitigation;
* whether the issue has already been disclosed elsewhere.

## Documentation contributions

Documentation improvements are welcome and are treated as contributions under the same CLA process.

Documentation should:

* use clear technical language;
* distinguish supported behaviour from planned behaviour;
* include complete commands and configuration examples;
* avoid including credentials, private endpoints or customer information;
* remain consistent with the current implementation.

## Code of conduct

Contributors must communicate professionally and respectfully.

Harassment, personal attacks, discriminatory behaviour and deliberate disruption are not acceptable. Maintainers may restrict participation where necessary to protect the project and its contributors.

## Questions

For contribution or licensing questions, contact:

[Mike Crooks](mailto:mike@rightstep-health.com)
Rightstep OÜ
