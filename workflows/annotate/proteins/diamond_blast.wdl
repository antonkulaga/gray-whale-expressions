version development

workflow Diamond_Blast {
    input {
      Int threads
      File db
      File query
      String result_name
      String results_folder
      String mode
      String output_format
    }

  call diamond_blast {
      input:
        threads = threads,
        database = db,
        name = result_name,
        query = query,
        mode = mode,
        output_format = output_format
    }

  call copy as copy_results {
    input:
        files = [diamond_blast.out],
        destination = results_folder
  }

  output {
       File out = copy_results.out[0]
  }

}

task diamond_blast {
    input {
      Int threads
      File database
      File query
      String name
      String mode
      String output_format
    }

    command {
        diamond ~{mode} -d ~{database}  -q ~{query} \
          --more-sensitive -o ~{name}.m8 \
          -f ~{output_format}
     }
    #	qseqid means Query Seq - id
    #	qlen means Query sequence length
    #	sseqid means Subject Seq - id
    #	sallseqid means All subject Seq - id(s), separated by a ';'
    #	slen means Subject sequence length
    #	qstart means Start of alignment in query
    #	qend means End of alignment in query
    #	sstart means Start of alignment in subject
    #	send means End of alignment in subject
    #	qseq means Aligned part of query sequence
    #	sseq means Aligned part of subject sequence
    #	evalue means Expect value
    #	bitscore means Bit score
    #	score means Raw score
    #	length means Alignment length
    #	pident means Percentage of identical matches
    #	nident means Number of identical matches
    #	mismatch means Number of mismatches
    #	positive means Number of positive - scoring matches
    #	gapopen means Number of gap openings
    #	gaps means Total number of gaps
    #	ppos means Percentage of positive - scoring matches
    #	qframe means Query frame
    #	btop means Blast traceback operations(BTOP)
    #	staxids means unique Subject Taxonomy ID(s), separated by a ';' (in numerical order)
    #	stitle means Subject Title
    #	salltitles means All Subject Title(s), separated by a '<>'
    #	qcovhsp means Query Coverage Per HSP
    #	qtitle means Query title

  runtime {
    docker: "quay.io/comp-bio-aging/diamond:master"
  }

  output {
       File out = name + ".m8"
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