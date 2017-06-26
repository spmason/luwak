node ('analytics-backend') {
    stage 'Checkout'
    checkout([
        $class: 'GitSCM',
        branches: scm.branches,
        doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
        extensions: scm.extensions + [[$class: 'CleanCheckout']],
        userRemoteConfigs: scm.userRemoteConfigs
      ])

    stage 'Test'
    withEnv(["PATH+MAVEN=${tool 'm3'}/bin"]) {
      sh "mvn -B -Dsource.skip -Dmaven.javadoc.skip clean test findbugs:findbugs"
      step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
      step([$class: 'FindBugsPublisher', pattern: '**/target/findbugsXml.xml'])
    }
}
