rule create_euregio_csv:
    input:
        expand("{euregio_dir}/EURegionalIOtable_{year}.ods",euregio_dir=config["EUREGIO_DIR"], year=[2006,2007,2008,2009,2010])
    output:
        expand("{euregio_dir}/EURegionalIOtable_{year}.csv",euregio_dir=config["EUREGIO_DIR"], year=[2006,2007,2008,2009,2010])
    params:
        eu_dir=config["EUREGIO_DIR"]
    shell:
        """
        cd params.eu_dir;
        for YEAR in 2006 2007 2008 2009 2010
        do
            libreoffice --convert-to xlsx EURegionalIOtable_$YEAR.ods;
            xlsx2csv --sheetname $YEAR EURegionalIOtable_$YEAR.xlsx EURegionalIOtable_$YEAR.csv;
        done
        """

rule create_euregio_pkl:
    input:
        io_files = expand("{inputdir}/euregio/{filenames}_{{year}}.csv",inputdir=config["SOURCE_DATA_DIR"],filenames=["Z","VA","Y"]),
        index_files = expand("{inputdir}/euregio/{filenames}_index.csv",inputdir=config["SOURCE_DATA_DIR"],filenames=["regions","sectors","va","fd"])
    conda:
        "../env/boario-use.yml"
    output:
        mrioout = expand("{outputdir}/mrios/euregio/euregio_{{year}}_full.pkl",outputdir=config["BUILDED_DATA_DIR"])
    resources:
        vmem_mb=8000,
        mem_mb=7000,
        disk_mb=2000
    shell:
        """
        python {config[INPUTS_GENERATION_SCRIPTS_DIR]}/create_euregio_pkl.py --input-folder {config[SOURCE_DATA_DIR]}/euregio --year {wildcards.year} --output {output}
        """
