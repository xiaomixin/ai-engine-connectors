# AI Engine Connectors

Parent Maven project for AI Engine connectors built for Apache Flink.

## Project Structure

```
ai-engine-connectors/
├── pom.xml                          # Parent POM
└── flink-connector-websocket/       # WebSocket source connector submodule
    ├── pom.xml
    └── src/
        ├── main/
        │   ├── java/
        │   └── scala/
        └── test/
            ├── java/
            └── scala/
```

## Technology Stack

- **Java**: 17
- **Scala**: 2.12
- **Apache Flink**: 2.1.1
- **Build Tool**: Maven

## Modules

### flink-connector-websocket

WebSocket source connector for Apache Flink. This module implements a source connector that can read data from WebSocket endpoints.

## Building the Project

To build all modules:

```bash
mvn clean install
```

To build a specific module:

```bash
cd flink-connector-websocket
mvn clean install
```

## Adding New Connectors

To add a new connector submodule:

1. Create a new directory under the root: `ai-engine-<connector-name>`
2. Add the module to the parent `pom.xml` in the `<modules>` section
3. Create a `pom.xml` in the new module directory with the parent reference

