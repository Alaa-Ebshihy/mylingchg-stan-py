#!/bin/bash
INPUT_DIR=$1
WORKING_DIR=$2
OUTPUT_DIR=$3
STARTTIMEPOINT=$4
ENDTIMEPOINT=$5
STEP=$6
MODEL_FAMILY=$7
KNN=$8
FILTER_VOCAB_FILE=${9}
BOOTSTRAP=${10}
THRESHOLD=${11}
WORKERS=${12}

EMBEDDINGS_TYPE=skipgram
echo "Output directory is", $OUTPUT_DIR

mkdir -p $WORKING_DIR
mkdir -p $OUTPUT_DIR

echo "Mapping to joint space"
mkdir -p $WORKING_DIR/predictors
echo "Predictors will be stored in", $WORKING_DIR/predictors
arr=("$INPUT_DIR/*.model")
((FINALTIMEPOINT=$ENDTIMEPOINT-$STEP))
parallel -j${WORKERS} python learn_map.py -k ${KNN} -f $WORKING_DIR/predictors/{/.}.predictor -o {} -n {//}/${FINALTIMEPOINT}_*.model -m $MODEL_FAMILY ::: $arr