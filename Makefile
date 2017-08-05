STATES=1
TGS=1
SERIES=true
OUTPUT=report.json

build:
	mvn -f tspoon/pom.xml clean package -Pbuild-jar -DskipTests

# Runs the build in a local cluster supposing that Flink is in the path
run:
	flink run tspoon/target/tspoon-research-artifact.jar --noStates $(STATES) --noTG $(TGS) \
		--series $(SERIES) --output $(OUTPUT) --nRec 10000 --sled 0 --isolationLevel 0

