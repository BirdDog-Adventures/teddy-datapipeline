# Multi-platform Docker build for AWS Lambda
# Based on the working pattern from birddog-geodata-viewer

FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.11

# Copy Lambda function code
COPY lambda/ ${LAMBDA_TASK_ROOT}/

# Copy RSA keys for JWT authentication
RUN mkdir -p ${LAMBDA_TASK_ROOT}/keys
COPY keys/snowflake_rsa_key.p8 ${LAMBDA_TASK_ROOT}/keys/

# Install dependencies with platform targeting
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables for JWT authentication
ENV SNOWFLAKE_PRIVATE_KEY_PATH=${LAMBDA_TASK_ROOT}/keys/snowflake_rsa_key.p8
ENV SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT

# Lambda handler
CMD ["api_parcel_ingestion.lambda_handler"]