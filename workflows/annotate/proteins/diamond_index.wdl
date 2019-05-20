version development

workflow Diamond_Index {

    input {
        File db
        String name
        String results_folder
    }

  call diamond_index {
    input:
      fasta = db,
      name = name
  }

  call copy as copy_results {
    input:
        files = [diamond_index.out],
        destination = results_folder
  }

  output {
    Array[File] results = copy_results.out
  }

}

task diamond_index {
    input {
        File fasta
        String name
    }

    command {
        diamond makedb --in ${fasta} -d ${name}
     }

  runtime {
    docker: "quay.io/comp-bio-aging/diamond:master"
  }

  output {
       File out = name + ".dmnd"
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