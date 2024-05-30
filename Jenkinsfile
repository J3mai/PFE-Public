pipeline {
    agent any

    environment {
        DOCKER_IMAGE = 'rental_tool'
    }

    stages {
        stage('Checkout') {
            steps {
                // Checkout code from Git using the personal access token
                git(
                    url: 'https://github.com/J3mai/PFE.git', 
                    branch: 'master', 
                )
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    // Build Docker image
                    sh 'docker build -t ${DOCKER_IMAGE} .'
                }
            }
        }

        stage('Run Docker Container') {
            steps {
                script {
                    // Run Docker container
                    sh 'docker run --rm ${DOCKER_IMAGE}'
                }
            }
        }
    }

    post {
        always {
            script {
                // Clean up Docker images to free up space
                sh "docker rmi ${DOCKER_IMAGE} || true"
            }
            // Clean workspace after build
            cleanWs()
        }
    }
}
