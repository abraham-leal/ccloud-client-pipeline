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
   stage('Assembling') {
        steps {
          echo 'Assembling...'
          sh 'mvn assembly:assembly'
        }
   }

   stage('Deploy') {
        steps {
          echo 'Deploying...'
          sh 'sudo bash client-run-class.sh --producer-props ~/secrets/producer.properties --topic myinternaltopic'
        }
   }
  }
}