version development

workflow quality_de_novo {
input{
  Int threads
  Int min_len
  Int q
  String sra
  String results_folder #will be created if needed
  String adapter = "TruSeq3-PE"
}

  call download as download_sra {
    input:
        sample = sra
  }

  call report as initial_report_1 {
      input:
          sampleName = basename(download_sra.reads_1, ".fastq.gz"),
          file = download_sra.reads_1
  }

  call report as initial_report_2 {
      input:
          sampleName = basename(download_sra.reads_2, ".fastq.gz"),
          file = download_sra.reads_2
  }


  call trimmomatics {
      input:
        reads_1 = download_sra.reads_1,
        reads_2 = download_sra.reads_2,
        min_len = min_len,
        q = q,
        threads = threads,
        adapter = adapter
  }

  call report as report_trimmomatics_1 {
      input:
        sampleName = basename(trimmomatics.out1, ".fastq.gz"),
        file = trimmomatics.out1
      }

  call report as report_trimmomatics_2 {
      input:
        sampleName = basename(trimmomatics.out2, ".fastq.gz"),
        file = trimmomatics.out2
      }

  call copy as copy_trimmed {
    input:
        files = [trimmomatics.out1, trimmomatics.out2],
        destination = results_folder + "/trimmed/"
  }

  call copy as copy_initial_quality_reports {
    input:
        files = [initial_report_1.out, initial_report_2.out],
        destination = results_folder + "/quality/initial/"
  }

  call copy as copy_cleaned_quality_reports {
    input:
        files = [report_trimmomatics_1.out, report_trimmomatics_2.out],
        destination = results_folder + "/quality/cleaned/"
  }

  call multi_report {
    input:
        last_reports = copy_cleaned_quality_reports.out,
        folder = results_folder,
        report = "reports"
  }

  call copy as copy_multi_report {
      input:
          files = [multi_report.out],
          destination = results_folder
    }


  output {
    Array[File] out = copy_multi_report.out
  }

}


task report {
input{
  String sampleName
  File file

}

  command {
    /opt/FastQC/fastqc ${file} -o .
  }

  runtime {
    docker: "quay.io/ucsc_cgl/fastqc@sha256:86d82e95a8e1bff48d95daf94ad1190d9c38283c8c5ad848b4a498f19ca94bfa"
  }

  output {
    File out = sampleName+"_fastqc.zip"
  }
}

task multi_report {
input {
   File folder
   String report
   Array[File] last_reports #just a hack to make it wait for the folder to be created


}
   command {
        multiqc ${folder} --outdir ${report}
   }

   runtime {
        docker: "quay.io/comp-bio-aging/multiqc@sha256:20a0ff6dabf2f9174b84c4a26878fff5b060896a914d433be5c14a10ecf54ba3"
   }

   output {
        File out = report
   }
}


task trimmomatics {
input {
    File reads_1
    File reads_2
    Int q
    Int min_len
    Int threads
    String adapter
}


    command {
       /usr/local/bin/trimmomatic PE \
            ${reads_1} \
            ${reads_2} \
            ${basename(reads_1, ".fastq.gz")}_trimmed.fastq.gz \
            ${basename(reads_1, ".fastq.gz")}_trimmed_unpaired.fastq.gz \
            ${basename(reads_2, ".fastq.gz")}_trimmed.fastq.gz \
            ${basename(reads_2, ".fastq.gz")}_trimmed_unpaired.fastq.gz \
            -threads ${threads} \
            ILLUMINACLIP:/usr/local/share/trimmomatic/adapters/${adapter}.fa:2:30:10:1:TRUE \
            SLIDINGWINDOW:4:${q} MINLEN:${min_len}
    }

    runtime {
        docker: "quay.io/biocontainers/trimmomatic@sha256:bf4b0b2d2747670deeb9a6adc4a50aa923b830f0b02be47e82d1b848e1368078"
    }

    output {
        File out1 = basename(reads_1, ".fastq.gz") + "_trimmed.fastq.gz"
        File out1_unpaired = basename(reads_1, ".fastq.gz") + "_trimmed_unpaired.fastq.gz"
        File out2 = basename(reads_2, ".fastq.gz") + "_trimmed.fastq.gz"
        File out2_unpaired = basename(reads_2, ".fastq.gz") + "_trimmed_unpaired.fastq.gz"
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

task download {

input{
    String sample
}

    # read the following explanations for parameters
    # https://edwards.sdsu.edu/research/fastq-dump/

    command {
        fastq-dump --skip-technical --gzip --readids --read-filter pass --dumpbase --split-files --clip ${sample}
    }

    runtime {
        docker: "quay.io/biocontainers/sra-tools@sha256:6d8c1daefedf6a4d00b3351d030228ca7cc4669f73f5fb76d76d1b80e79905f1"
    }

    output {
        File reads_1 = sample + "_pass_1.fastq.gz"
        File reads_2 = sample + "_pass_2.fastq.gz"
    }
}