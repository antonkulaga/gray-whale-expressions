version development

workflow quant_index {

    input {
      File transcriptome
      String name
      String indexes_folder #will be created if needed
      String salmon_max_memory = "28G"
      Int salmon_index_threads = 14
    }

    call salmon_index  {
        input:
          transcriptomeFile = transcriptome,
          indexName =  name,
          max_memory = salmon_max_memory,
          p = salmon_index_threads
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
    input {
        File transcriptomeFile
        String indexName
        Int p
        String max_memory
    }

  command {
    salmon index -t ~{transcriptomeFile} -p ~{p} -i ~{indexName}
  }

  runtime {
    docker: "quay.io/biocontainers/salmon@sha256:0aea466ba3eae62cb3ea8077e30b6212ca152f73e9520ca19f7421c5be519ef9" #1.1.0--hf69c8f4_0
    maxRetries: 3
    docker_memory: "${max_memory}"
    docker_cpu: "${p}"
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