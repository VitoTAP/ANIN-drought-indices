# sudo docker build -t anin_drought_indices .
# sudo docker run -it --privileged --network host anin_drought_indices

FROM python:3.11.9

ENV HOME=/root
RUN mkdir -p $HOME/ANIN-drought-indices && \
    cd $HOME/ANIN-drought-indices
WORKDIR $HOME/ANIN-drought-indices
# RUN git clone --recursive https://github.com/VitoTAP/ANIN-drought-indices.git

COPY . $HOME/ANIN-drought-indices

RUN cd $HOME/ANIN-drought-indices && python -m pip install -r requirements.txt --default-timeout=100

CMD [ "jupyter", "notebook", "--allow-root" ]
EXPOSE 8888
