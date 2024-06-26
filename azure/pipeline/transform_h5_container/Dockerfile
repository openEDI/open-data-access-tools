FROM continuumio/miniconda3

# make docker use bash instead of sh
SHELL ["/bin/bash", "--login", "-c"]

# create environment
COPY ./pipeline/transform_h5_container/env.yml .
RUN conda env create -f env.yml

# install azcopy
COPY ./pipeline/transform_h5_container/install_azcopy.sh .
RUN sh install_azcopy.sh

# copy all necessary files
COPY ./.sas .
COPY ./pipeline/transform_h5_container/* ./
COPY ./pipeline/etl_tools.py .
COPY ./pipeline/aws_tools.py .

# make entrypoint script executable
RUN chmod u+x entrypoint.sh

# activate environment and run container
ENTRYPOINT ["./entrypoint.sh"]
CMD ["python", "transform.py"]
