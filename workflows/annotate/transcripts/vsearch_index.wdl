version development

workflow vsearch_index {
input{
  File db
  String name
  String indexes_folder = "/data/indexes/vsearch"

}

  call vsearch_make_index {
    input:
      fasta = db,
      name = name
  }

  call copy as copy_results {
    input:
        files = [vsearch_make_index.out],
        destination = indexes_folder
  }

  output {
    Array[File] results = copy_results.out
  }

}

task vsearch_make_index {
input{
  File fasta
  String name

}

    command {
        vsearch --makeudb_usearch ${fasta} --output ${name}.udb
        chmod -R o+rwx ${name}.udb
     }

  runtime {
    docker: "quay.io/comp-bio-aging/vsearch:latest"
  }

  output {
       File out = name + ".udb"
       String db_name = name
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