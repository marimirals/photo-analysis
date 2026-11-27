# Образ analyzer (Python):
#   docker build -f deploy/docker/analyzer.Dockerfile -t photo-analyzer:dev .
#
# Особенность: mediapipe ставит обычный OpenCV, которому нужны системные
# графические библиотеки (libGL). Ставить их через apt не хочется (да и
# deb.debian.org с ноутбука недоступен), поэтому меняем OpenCV на
# headless-сборку той же версии — она для серверов и ничего системного не требует.

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=60 \
    PIP_RETRIES=10 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY analyzer/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt && \
    CV_VERSION="$(pip show opencv-contrib-python | sed -n 's/^Version: //p')" && \
    echo "opencv version: $CV_VERSION" && \
    pip uninstall -y opencv-contrib-python && \
    pip install "opencv-contrib-python-headless==${CV_VERSION}" && \
    python -c "import cv2, mediapipe; from mediapipe.tasks.python import vision; print('imports ok, cv2', cv2.__version__)"

# Код, модели и proto (из него генерируется gRPC-код).
COPY proto /proto
COPY analyzer/ /app/
RUN test -f models/face_detection_yunet_2023mar.onnx && test -f models/face_landmarker.task \
    || (echo "нет моделей в analyzer/models — запусти download_models.py" && exit 1)
RUN python gen_proto.py

RUN useradd --uid 10001 --no-create-home analyzer
USER 10001
EXPOSE 50051 9102
CMD ["python", "-m", "photoanalyzer.server"]
