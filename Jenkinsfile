#!/usr/bin/env groovy

String getMavenAgent(Integer mavenCpuLimit = 3){
  // assuming one core left for main maven thread
  String mavenForkCount = mavenCpuLimit - 1;
  // assuming 2Gig for each core
  String mavenMemoryLimit = mavenCpuLimit * 2;
  """
metadata:
  labels:
    agent: ci-cambpm-camunda-cloud-build
spec:
  nodeSelector:
    cloud.google.com/gke-nodepool: agents-n1-standard-32-netssd-preempt
  tolerations:
  - key: "agents-n1-standard-32-netssd-preempt"
    operator: "Exists"
    effect: "NoSchedule"
  containers:
  - name: maven
    image: maven:3.6.3-openjdk-8
    command: ["cat"]
    tty: true
    env:
    - name: LIMITS_CPU
      value: ${mavenForkCount}
    - name: TZ
      value: Europe/Berlin
    resources:
      limits:
        cpu: ${mavenCpuLimit}
        memory: ${mavenMemoryLimit}Gi
      requests:
        cpu: ${mavenCpuLimit}
        memory: ${mavenMemoryLimit}Gi
  """
}

pipeline{
  agent none
  stages{
    stage("Compile all the things!"){
      agent {
        kubernetes {
          yaml getMavenAgent()
        }
      }
      steps{
        container("maven"){
          // Install dependencies
          sh '''
            curl -s -O https://deb.nodesource.com/node_14.x/pool/main/n/nodejs/nodejs_14.6.0-1nodesource1_amd64.deb
            dpkg -i nodejs_14.6.0-1nodesource1_amd64.deb
            npm set unsafe-perm true
            apt -qq update && apt install -y g++ make
          '''
          // Run maven
          configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
            sh """
              mvn -s \$MAVEN_SETTINGS_XML -B -T\$LIMITS_CPU clean source:jar install -D skipTests -Dmaven.repo.local=\$(pwd)/.m2
            """
          }
          stash name: "artifactStash", includes: ".m2/org/camunda/**/*-SNAPSHOT/**", excludes: "**/*.zip,**/*.tar.gz"
        }
      }
    }
    stage("Before"){
      agent {
        kubernetes {
          yaml getMavenAgent()
        }
      }
      stages {
        stage('Unstash') {
          steps{
            container("maven"){
              unstash "artifactStash"
            }
          }
        }
        stage('to rename') {
          failFast true
          parallel {
            stage('XML model') {
              steps{
                container("maven"){
                  // Run maven
                  unstash "artifactStash"
                  configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                    sh """
                      export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                      cd model-api/xml-model && mvn -s \$MAVEN_SETTINGS_XML -B test
                    """
                  }
                }
              }
            }
            stage('BPMN model') {
              steps{
                container("maven"){
                  // Run maven
                  configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                    sh """
                      export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                      cd model-api/bpmn-model && mvn -s \$MAVEN_SETTINGS_XML -B test
                    """
                  }
                }
              }
            }
            stage('DMN model') {
              steps{
                container("maven"){
                  // Run maven
                  configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                    sh """
                      export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                      cd model-api/dmn-model && mvn -s \$MAVEN_SETTINGS_XML -B test
                    """
                  }
                }
              }
            }
            stage('CMMN model') {
              steps{
                container("maven"){
                  // Run maven
                  configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                    sh """
                      export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                      cd model-api/cmmn-model && mvn -s \$MAVEN_SETTINGS_XML -B test
                    """
                  }
                }
              }
            }
            stage('camunda-commons-typed-values tests') {
              steps{
                container("maven"){
                  // Run maven
                  configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                    sh """
                      export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                      cd typed-values && mvn -s \$MAVEN_SETTINGS_XML -B test
                    """
                  }
                }
              }
            }
            stage('DMN engine tests') {
              steps{
                container("maven"){
                  // Run maven
                  configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                    sh """
                      export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                      cd engine-dmn && mvn -s \$MAVEN_SETTINGS_XML -B verify
                    """
                  }
                }
              }
            }
            stage('sql-scripts') {
              agent {
                kubernetes {
                  yaml getMavenAgent()
                }
              }
              steps{
                container("maven"){
                  // Run maven
                  unstash "artifactStash"
                  configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                    //sh """
                    //  errors=0
                    //
                    //  for create_script in engine/src/main/resources/org/camunda/bpm/engine/db/create/*.sql; do
                    //      drop_script=${create_script//create/drop}
                    //      created_indexes=$(grep -i '^\s*create \(unique \)\?index' $create_script | tr [A-Z] [a-z] | sed 's/^\s*create \(unique \)\?index \(\S\+\).*$/\2/' | sort)
                    //      dropped_indexes=$(grep -i '^\s*drop index' $drop_script | tr [A-Z] [a-z] | sed 's/^\s*drop index \([^.]\+\.\)\?\([^ ;]\+\).*$/\2/' | sort)
                    //      diff_indexes=$(diff <(echo \'$created_indexes\') <(echo \'$dropped_indexes\'))
                    //      if [ $? -ne 0 ]; then
                    //          echo 'Found index difference for:'
                    //          echo $create_script
                    //          echo $drop_script
                    //          echo -e \'${diff_indexes}\n\'
                    //          errors=$[errors + 1]
                    //      fi
                    //  done
                    //  
                    //  exit $errors
                    //"""
                    sh """
                      export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                      cd distro/sql-script && mvn -s \$MAVEN_SETTINGS_XML -B install -Pcheck-sql,h2
                    """
                  }
                }
              }
            }
          }
        }
      }
    }
    stage('Run Tests') {
      failFast true
      parallel {
        stage('Engine unit tests') {
          agent {
            kubernetes {
              yaml getMavenAgent()
            }
          }
          steps{
            container("maven"){
              // Run maven
              unstash "artifactStash"
              configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                sh """
                  export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                  cd engine && mvn -s \$MAVEN_SETTINGS_XML -B -T\$LIMITS_CPU test -Pdatabase,h2
                """
              }
            }
          }
        }
        stage('QA: Instance Migration Tests') {
          agent {
            kubernetes {
              yaml getMavenAgent()
            }
          }
          steps{
            container("maven"){
              // Run maven
              unstash "artifactStash"
              configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                sh """
                  export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                  cd qa/test-db-instance-migration && mvn -s \$MAVEN_SETTINGS_XML -B verify -Pinstance-migration,h2
                """
              }
            }
          }
        }
        stage('QA: Rolling Update Tests') {
          agent {
            kubernetes {
              yaml getMavenAgent()
            }
          }
          steps{
            container("maven"){
              // Run maven
              unstash "artifactStash"
              configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                sh """
                  export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                  cd qa/test-db-rolling-update && mvn -s \$MAVEN_SETTINGS_XML -B verify -Prolling-update,h2
                """
              }
            }
          }
        }
        stage('QA: Upgrade old engine from 7.13') {
          agent {
            kubernetes {
              yaml getMavenAgent()
            }
          }
          steps{
            container("maven"){
              // Run maven
              unstash "artifactStash"
              configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                sh """
                  export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                  cd qa && mvn -s \$MAVEN_SETTINGS_XML -B -T\$LIMITS_CPU verify -Pold-engine,h2
                """
              }
            }
          }
        }
        stage('QA: Upgrade database from 7.13') {
          agent {
            kubernetes {
              yaml getMavenAgent()
            }
          }
          steps{
            container("maven"){
              // Run maven
              unstash "artifactStash"
              configFileProvider([configFile(fileId: 'maven-nexus-settings', variable: 'MAVEN_SETTINGS_XML')]) {
                sh """
                  export MAVEN_OPTS="-Dmaven.repo.local=\$(pwd)/.m2"
                  cd qa/test-db-upgrade && mvn -s \$MAVEN_SETTINGS_XML -B -T\$LIMITS_CPU verify -Pupgrade-db,h2
                """
              }
            }
          }
        }
      }
    }
  }
}
