pipeline {
    agent any

    stages {
        stage('Clean') {
            steps {
            echo 'Cleaning...'
            sh 'mvn clean'
            }
        }
        stage('Test') {
            steps {
            echo 'Testing...'
            sh 'mvn test'
            }
        }
        stage('Compile') {
            steps {
            echo 'Compiling...'
            sh 'mvn compile'
        }
    }
}