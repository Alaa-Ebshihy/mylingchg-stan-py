#!/bin/bash
INPUT_DIR=$1
WORKING_DIR=$2
OUTPUT_DIR=$3
STARTTIMEPOINT=$4
ENDTIMEPOINT=$5
STEP=$6
MODEL_FAMILY=$7
MODEL_EXT=$8
KNN=$9
FILTER_VOCAB_FILE=${10}
BOOTSTRAP=${11}
THRESHOLD=${12}
WORKERS=${13}

EMBEDDINGS_TYPE=skipgram
echo "Output directory is", $OUTPUT_DIR

mkdir -p $WORKING_DIR
mkdir -p $OUTPUT_DIR

echo "Mapping to joint space"
mkdir -p $WORKING_DIR/predictors
echo "Predictors will be stored in", $WORKING_DIR/predictors
arr=("$INPUT_DIR/*.$MODEL_EXT")
((FINALTIMEPOINT=$ENDTIMEPOINT-$STEP))
parallel -j${WORKERS} python learn_map.py -k ${KNN} -f $WORKING_DIR/predictors/{/.}.predictor -o {} -n {//}/${FINALTIMEPOINT}_*.model -m $MODEL_FAMILY ::: $arr

WORDS_FILE=${FILTER_VOCAB_FILE}

echo "Computing displacements"
mkdir -p $WORKING_DIR/displacements/
export MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_DYNAMIC=FALSE
python embedding_displacements.py -f $WORDS_FILE -d $INPUT_DIR/ -p $WORKING_DIR/predictors/ -os words -es ".model" -ps ".predictor" -sy $STARTTIMEPOINT -ey $ENDTIMEPOINT -s $STEP -e $EMBEDDINGS_TYPE -o $WORKING_DIR/displacements/ -workers ${WORKERS}

echo "Creating time series"
mkdir -p $WORKING_DIR/timeseries/
python tsconstruction/dump_timeseries.py -f $WORKING_DIR/displacements/timeseries_s_t_words.pkl -s $WORKING_DIR/timeseries/source.csv -e $WORKING_DIR/timeseries/dest.csv -m $STARTTIMEPOINT -n $ENDTIMEPOINT -st $STEP -me "polar" -metric "cosine" -workers ${WORKERS}

python detect_changepoints_word_ts.py -f $WORKING_DIR/timeseries/source.csv -v $FILTER_VOCAB_FILE -p $OUTPUT_DIR/pvals_ts.csv -n $OUTPUT_DIR/samples_ts.csv -c $STARTTIMEPOINT -w ${WORKERS} -b ${BOOTSTRAP} -t ${THRESHOLD}

python demonstrate_cp.py -f $WORKING_DIR/timeseries/source.csv -p $OUTPUT_DIR/pvals_demon.csv -n $OUTPUT_DIR/samples_demon.csv -c $STARTTIMEPOINT -w ${WORKERS} -b ${BOOTSTRAP} -t ${THRESHOLD}
