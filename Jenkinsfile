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
      stage('PR Builder'){
      steps{
        sh "build/mvn install"
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
