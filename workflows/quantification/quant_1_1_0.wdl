version development

#bats samples are quaintified with 1.1.0 version of salmon
workflow quantification {
    input{
        Directory index
        Array[File] reads
        Int threads
        String results_folder
        String sample_name
        String salmon_max_memory = "30G"

    }


    call salmon {
        input:
            index = index,
            threads = threads,
            reads = reads,
            max_memory = salmon_max_memory,
            name = sample_name
    }

    call copy {
        input:
            destination = results_folder,
            files = [salmon.out]
    }

}

task salmon {
  input {
    Directory index
    Array[File] reads
    Int threads
    String name
    String max_memory
    Int bootstraps = 128
  }

  Boolean is_paired = true

  command {
    salmon --no-version-check quant -i ~{index} --numBootstraps ~{bootstraps} --threads ~{threads} -l A --seqBias --gcBias --validateMappings --writeUnmappedNames -o quant_~{name} \
    ~{if(is_paired) then "-1 " + reads[0] + " -2 "+ reads[1] else "-r " + reads[0]}
  }

  runtime {
    #docker: "quay.io/comp-bio-aging/salmon" #1.1.0--hf69c8f4_0
    docker: "quay.io/biocontainers/salmon@sha256:0aea466ba3eae62cb3ea8077e30b6212ca152f73e9520ca19f7421c5be519ef9" #1.1.0--hf69c8f4_0
    docker_memory: "${max_memory}"
    docker_cpu: "${threads}"
    maxRetries: 3
  }

  output {
    File out = "quant_" + name
    File lib = out + "/" + "lib_format_counts.json"
    File quant = out + "/" + "quant.sf"
    File quant_genes =  out + "/" + "quant.genes.sf"
  }
}

task copy {
    input {
        Array[File] files
        String destination
    }

    command {
        mkdir -p ~{destination}
        cp -L -R -u ~{sep=' ' files} ~{destination}
    }

    output {
        Array[File] out = files
    }
}