pipeline{
  agent{
    node { label 'master' }
  }
  options{
    timestamps()
    ansiColor('xterm')
  }
  triggers {
    issueCommentTrigger('.*jttp.*')
  }
  stages{
    stage('Checkout Source'){
      steps{
        checkout scm
      }
    }
    stage('Building'){
      steps{
        // Determine what kind of build we are building
        csdBuild env.JOB_NAME
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

def prBuilder(){
  def mvnArgs="-Pdeb -U -Phadoop-2.7 -Dhadoop.version=2.8.2 -Pkubernetes -Pyarn -Phive -Phive-thriftserver -Dpyspark -Dsparkr"
  stage('PR Build'){
    sh "build/mvn ${mvnArgs}  -DskipTests clean install"
  }
  stage('PR Test'){
    sh "build/mvn ${mvnArgs} install"
  }
}

def releaseBuilder(){
  def mvnArgs="-Pdeb -U -Phadoop-2.7 -Dhadoop.version=2.8.2 -Pkubernetes -Pyarn -Phive -Phive-thriftserver -Dpyspark -Dsparkr -DskipTests -Dgpg.skip=true -Dmaven.javadoc.skip=true"
  stage('Release Clean'){
    // We need to check out the branch as a "symbolic ref" (i.e. not as an SHA1), otherwise
    // Maven Release plugin won't be able to push to the branch.
    def branch_name=env.JOB_NAME.split('/')[-1]
    sh """
    git reset --hard HEAD
    git checkout ${branch_name}
    git fetch origin
    git reset --hard origin/${branch_name}
    git clean -dfx
    """

    sh "build/mvn ${mvnArgs} release:clean"
    pom = readMavenPom(file: 'pom.xml')
    sparkVersion = pom.version.replace("-SNAPSHOT", "")
  }
  stage('Release Prepare'){
    sh "build/mvn --batch-mode ${mvnArgs} -Darguments=\"${mvnArgs}\" release:prepare"
  }
  stage('Release Perform'){
    sh "build/mvn --batch-mode ${mvnArgs} -Darguments=\"${mvnArgs}\" release:perform"
  }
  stage('Stage Binaries'){
    sh """
    scp target/checkout/assembly/target/spark*_all.deb \
      mash@apt.clearstorydatainc.com:/var/repos/apt/private/dists/precise/misc/binary-all
    """
  }
  stage('Release Docker Image'){
    def repo = "628897842239.dkr.ecr.us-west-2.amazonaws.com"
    sh """
    dev/make-distribution.sh --name custom-spark --pip --tgz -Phadoop-2.7 -Phive -Phive-thriftserver -Pyarn -Pkubernetes -Dhadoop.version=2.8.2
    bin/docker-image-tool.sh -r ${repo} -t ${sparkVersion} build
    bin/docker-image-tool.sh -r ${repo} -t ${sparkVersion} push
    """
  }
}
def csdBuild(val) {
    switch (val) {
        case ~/.*PR.*/:
            prBuilder()
            break
        default:
            releaseBuilder()
            break
    }
}
