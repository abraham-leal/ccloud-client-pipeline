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

   stage('Dockerize') {
           steps {
             echo 'Dockerizing...'
             sh 'ls -l'
             sh 'bash docker build -t producer .'

           }
      }


   stage('Deploy') {
        steps {
          echo 'Deploying...'
          sh 'docker run -p 8081:8081 producer'
        }
   }
  }
}