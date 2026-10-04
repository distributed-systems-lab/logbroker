# Repository Guidelines

## Project Structure & Module Organization

This is a single-module Java 21/Maven distributed-log learning project. Production code lives in `src/main/java/vn/huyqt/logbroker/`: `storage` manages persistent logs; `broker` handles requests and partitions; `protocol` and `transport` provide codecs and Netty networking; `client` provides producer/consumer APIs; `controller` implements the three-voter metadata quorum. Examples live in `example` and `storage/example`.

Tests mirror these packages under `src/test/java`, with cross-component tests in `integration`. Golden protocol/storage vectors and fault seeds live in `src/test/resources`. Configuration samples are in `config/`; contracts, operational instructions, designs, and plans are in `docs/`. Maven output belongs in `target/`.

## Build, Test, and Development Commands

Use JDK 21 and Maven 3.9.x from the repository root:

- `mvn clean verify`: clean, compile, and run the full test suite.
- `mvn test`: run tests without cleaning build output.
- `mvn -Dtest=BrokerConfigTest test`: run a focused test class.
- `mvn verify dependency:copy-dependencies`: verify and prepare dependencies for local launches.
- `mvn spotless:apply`: format all production and test Java sources.
- `mvn spotless:check`: check formatting without changing files; also runs during `verify`.

Start a broker in PowerShell:

```powershell
java -cp "target/classes;target/dependency/*" vn.huyqt.logbroker.broker.BrokerMain --data target/broker-data --port 9092
```

Use `:` instead of `;` for Unix classpaths. Follow `docs/controller-configuration.md` for controller formatting and startup.

## Coding Style & Naming Conventions

Match surrounding Java code: four-space indentation, same-line opening braces, `PascalCase` types, `camelCase` methods/fields, and `UPPER_SNAKE_CASE` constants. Keep packages beneath `vn.huyqt.logbroker`. Document public contracts, durability guarantees, and resource ownership in Javadoc. Spotless in `pom.xml` pins google-java-format with AOSP style and LF line endings; it orders imports and removes unused imports. Use `scripts/format.ps1` or `scripts/format.sh` with `apply` or `check`, and avoid unrelated manual reformatting.

## Testing Guidelines

Use JUnit Jupiter 5 and Maven Surefire. Name test classes `*Test` and methods descriptively, such as `labDefaultsRespectFrameAndFetchBudgets`. Add regression coverage for changed behavior; reuse deterministic schedulers, fault harnesses, and checked-in vectors. No numeric coverage threshold is configured.

Run strict controller durability/process verification from Linux/WSL ext4; native Windows tests do not establish durability support. See `docs/controller-verification.md` for fault campaigns and platform limitations.

## Commit & Pull Request Guidelines

Follow the prevalent scoped style: `fix(controller): bound admission` or `docs(storage): clarify recovery`. Keep commits focused. PRs should explain behavior changes, link relevant issues or design documents, list validation commands/results and platform, and update affected protocol, storage, or configuration documentation. Keep generated logs and runtime data out of commits.
