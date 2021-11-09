gcloud dataproc jobs submit pyspark \
--region us-central1 \
--cluster cluster-igti-otacilio \
--py-files gs://igti-bootcamp-otacilio/scripts/src.zip \
gs://igti-bootcamp-otacilio/scripts/app.py \
-- cloud