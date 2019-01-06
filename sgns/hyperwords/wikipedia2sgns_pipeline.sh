#!/usr/bin/env bash

CORPUS_DIR=$1
PAIRS_DIR=$2
COUNTS_DIR=$3
OUTPUT_VEC_DIR=$4
START=$5
END=$6
STEP=$7

echo Creating pairs of year "${START}"
python hyperwords/corpus2pairs.py --win 5 --dyn --sub 1e-5 --del "${CORPUS_DIR}"/"${START}"_text8 > "${PAIRS_DIR}"/"${START}"_pairs

echo Creating counts of year "${START}"
scripts/pairs2counts.sh "${PAIRS_DIR}"/"${START}"_pairs> "${COUNTS_DIR}"/"${START}"_counts

python hyperwords/counts2vocab.py "${COUNTS_DIR}"/"${START}"_counts

echo SGNS of year "${START}"
word2vecf/word2vecf -train "${PAIRS_DIR}"/"${START}"_pairs -output "${OUTPUT_VEC_DIR}"/"${START}"-w -dumpcv "${OUTPUT_VEC_DIR}"/"${START}"-c -threads 25 -size 300 -sample 0 -negative 5 -wvocab "${COUNTS_DIR}"/"${START}"_counts.words.vocab -cvocab "${COUNTS_DIR}"/"${START}"_counts.contexts.vocab -verbose 2

START=`expr $START  + $STEP`
for ((i=START;i<=END;i+=STEP)); do

    echo Creating pairs of year "${i}"
    python hyperwords/corpus2pairs.py --win 5 --dyn --sub 1e-5 --del "${CORPUS_DIR}"/"${i}"_text8 > "${PAIRS_DIR}"/"${i}"_pairs

    echo Creating counts of year "${i}"
    scripts/pairs2counts.sh "${PAIRS_DIR}"/"${i}"_pairs> "${COUNTS_DIR}"/"${i}"_counts

    python hyperwords/counts2vocab.py "${COUNTS_DIR}"/"${i}"_counts

    LAST=`expr $i  - $STEP`

    echo SGNS of year "${i}"
    word2vecf/word2vecf -train "${PAIRS_DIR}"/"${i}"_pairs -output "${OUTPUT_VEC_DIR}"/"${i}"-w -dumpcv "${OUTPUT_VEC_DIR}"/"${i}"-c  -w-init-file "${OUTPUT_VEC_DIR}"/"${LAST}"-w.bin -c-init-file "${OUTPUT_VEC_DIR}"/"${LAST}"-c.bin -threads 25 -size 300 -sample 0 -negative 5 -wvocab "${COUNTS_DIR}"/"${i}"_counts.words.vocab -cvocab "${COUNTS_DIR}"/"${i}"_counts.contexts.vocab -verbose 2

done

