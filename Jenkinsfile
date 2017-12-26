pipeline{
  agent{
    node { label 'master' }
  }
  options{
    timestamps()
    ansiColor('xterm')
  }
  stages{
    stage('Checkout Source'){
      steps{
        checkout scm
      }
    }
    stage('Clean Install'){
      steps{
        sh "build/mvn -Pdeb -U -Phadoop-2.7 -Dhadoop.version=2.8.2 -Pkinesis-asl -Pyarn -Phive -Phive-thriftserver -Dpyspark -Dsparkr -DskipTests clean install"
      }
    }
    stage('Run tests'){
      steps{
        sh "build/mvn -Pdeb -U -Phadoop-2.7 -Dhadoop.version=2.8.2 -Pkinesis-asl -Pyarn -Phive -Phive-thriftserver -Dpyspark -Dsparkr install"
      }
    }
  }
  post{
    success{
      build job: 'jenkins-merge-when-green',
            parameters: [
              string(name: 'jenkinsJob', value: env.JOB_NAME ),
              string(name: 'jobNumber', value: env.BUILD_NUMBER),
              string(name: 'repo', value: 'spark')
            ],
            wait: false
    }
  }
}

def set_properties(){
  // Enabling JTTP on builds
  properties([
    disableConcurrentBuilds(),
    pipelineTriggers([
    issueCommentTrigger('.*jttp.*')
    ])
  ])
}
