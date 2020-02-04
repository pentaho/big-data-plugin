def GetScmProjectName() {
    return scm.getUserRemoteConfigs()[0].getUrl().tokenize('/').last().split("\\.")[0]
}

pipeline {
    agent any
    environment {
        GITHUB_TOKEN = credentials('9e494763-394a-4837-a25f-c1e9e61a7289')
        repo         = GetScmProjectName()
        version      = 'nightly'
        dist         = '9.0.0.0-423'
        username     = 'HiromuHota'
    }
    stages {
        stage('Build') {
            steps {
                sh 'mvn -DskipTests clean source:jar package -pl kettle-plugins/hadoop-cluster/ui'
            }
        }
        stage('Test') {
            steps {
                sh 'mvn test -pl kettle-plugins/hadoop-cluster/ui'
            }
        }
        stage('Deliver') {
            steps {
                sh '''
                    github-release delete --user $username --repo $repo --tag webspoon/$version || true
                    git tag -f webspoon/$version
                    git push -f https://${GITHUB_TOKEN}@github.com/$username/$repo.git webspoon/$version
                    github-release release --user $username --repo $repo --tag webspoon/$version --name "webSpoon/$version" --description "Auto-build by Jenkins on $(date +'%F %T %Z')" --pre-release
                    github-release upload --user $username --repo $repo --tag webspoon/$version --name "hadoop-cluster-ui-$dist.jar" --file kettle-plugins/hadoop-cluster/ui/target/hadoop-cluster-ui-$dist.jar
                    github-release upload --user $username --repo $repo --tag webspoon/$version --name "hadoop-cluster-ui-$dist-sources.jar" --file kettle-plugins/hadoop-cluster/ui/target/hadoop-cluster-ui-$dist-sources.jar
                '''
            }
        }
    }
}
