#!groovy
def BN = BRANCH_NAME == "master" || BRANCH_NAME.startsWith("releases/") ? BRANCH_NAME : "master"

library "knime-pipeline@$BN"

properties([
    pipelineTriggers([
        upstream('knime-bigdata/' + env.BRANCH_NAME.replaceAll('/', '%2F')),
    ]),
    parameters(workflowTests.getConfigurationsAsParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    knimetools.defaultTychoBuild('org.knime.update.orc')

    workflowTests.runTests(
        dependencies: [
            repositories: ['knime-orc', 'knime-bigdata-externals', 'knime-cloud',
                'knime-python', 'knime-bigdata', 'knime-filehandling', 'knime-streaming',
                'knime-kerberos', 'knime-textprocessing', 'knime-dl4j', 'knime-database',
                'knime-pmml-translation', 'knime-ensembles', 'knime-distance'],
        ],
    )

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        workflowTests.runSonar()
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}
/* vim: set shiftwidth=4 expandtab smarttab: */
