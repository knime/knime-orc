#!groovy
def BN = BRANCH_NAME == "master" || BRANCH_NAME.startsWith("releases/") ? BRANCH_NAME : "master"

library "knime-pipeline@$BN"

properties([
    pipelineTriggers([
        upstream('knime-bigdata/' + env.BRANCH_NAME.replaceAll('/', '%2F')),
    ]),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    knimetools.defaultTychoBuild('org.knime.update.orc')

    // workflowTests.runTests(
    //     dependencies: [
    //         repositories: ['knime-orc'],
    //     ],
    //     // this is optional and defaults to false
    //     withAssertions: true,
    // )

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
		// TODO: remove empty list once workflow tests are enabled
        workflowTests.runSonar([])
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}
/* vim: set shiftwidth=4 expandtab smarttab: */