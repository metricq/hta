FROM rikorose/gcc-cmake:gcc-8
LABEL maintainer="franz.hoepfner@tu-dresden.de"

RUN useradd -m hta
RUN apt-get update && apt-get install -y --no-install-recommends git build-essential  libboost-program-options-dev libboost-system-dev libboost-timer-dev parallel

COPY --chown=hta:hta . /home/hta/hta

RUN mkdir /home/hta/hta/build

WORKDIR /home/hta/hta/build
RUN cmake -DCMAKE_BUILD_TYPE=Release .. && make -j 2