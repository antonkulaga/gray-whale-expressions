version development

workflow quant_index {
input {
  File transcriptome
  String name
  String indexes_folder #will be created if needed
}
  call salmon_index  {
      input:
          transcriptomeFile = transcriptome,
          indexName =  name
  }

  call copy {
    input:
        files = [salmon_index.out],
        destination = indexes_folder
  }

  output {
    Array[File] out = copy.out
  }

}

task salmon_index {
input{
  File transcriptomeFile
  String indexName

}

  command {
    salmon index -t ${transcriptomeFile} -i ${indexName} --type quasi
  }

  runtime {
    docker: "combinelab/salmon@sha256:8a5f0de02b0df1b2571f8200e276c09ef1dd499ca13a883577230d85d8e644c3"
  }

  output {
    File out = indexName
  }

}


task copy {
    input {
        Array[File] files
        String destination
    }

    String where = sub(destination, ";", "_")

    command {
        mkdir -p ~{where}
        cp -L -R -u ~{sep=' ' files} ~{where}
        declare -a files=(~{sep=' ' files})
        for i in ~{"$"+"{files[@]}"};
          do
              value=$(basename ~{"$"}i)
              echo ~{where}/~{"$"}value
          done
    }

    output {
        Array[File] out = read_lines(stdout())
    }
}