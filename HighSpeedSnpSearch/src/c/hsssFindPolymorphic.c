#include <stdio.h>
#include <stdint.h>
#include <string.h> 

// using global variables to cut down on stack pushing operations.
FILE *f1;
FILE *f2;
int polymorphismThreshold;
int unknownsThreshold;
int strainCount;
char *sourceIdPrefix;

// input strains
int16_t seq1;
int16_t *seq1_p = &seq1;
int32_t loc1;
int32_t *loc1_p = &loc1;
char a1;  // allele
char *a1_p = &a1;
char p1;  // product
char *p1_p = &p1;
int8_t strain1;  
int8_t *strain1_p = &strain1;

// reference genome
int16_t refSeq = 0;
int16_t *refSeq_p = &refSeq;
int32_t refLoc = 0;
int32_t *refLoc_p = &refLoc;
char refAllele; 
char *refAllele_p = &refAllele;
char refProduct;
char *refProduct_p = &refProduct;

int a_count = 0;
int c_count = 0;
int g_count = 0;
int t_count = 0;
int U_count = 0; // unknown
int sumCount = 0;
int prevProduct;
int nonSyn = 0;

static inline int readStrainRow() {
	fread(seq1_p, 2, 1, f1);  
	fread(loc1_p, 4, 1, f1);  
	fread(a1_p, 1, 1, f1); 
	fread(p1_p, 1, 1, f1);
	return fread(strain1_p, 2, 1, f1);
}

static inline getRefGenomeInfo(int16_t seq, int32_t loc) {
	// refGenome file is a strain file: one row per SNP, showing the ref genomes values
	// advance through SNPs to our current one
	while(refSeq != seq && refLoc != loc) {
		fread(refSeq_p, 2, 1, f2);  
		fread(refLoc_p, 4, 1, f2);  
		fread(refAllele_p, 1, 1, f2); 
		fread(refProduct_p, 1, 1, f2);
	}
}

static inline updateCounts() {
	if (a1 == 0 && p1 == -1) {
		U_count += strain1;
		sumCount += strain1;
	} else {
		if (a1 == 1) a_count++;
		else if (a1 == 2) c_count++;
		else if (a1 == 3) g_count++;
		else if (a1 == 4) t_count++;
		else U_count++;
		sumCount++;
	}
	if (p1 != prevProduct && p1 > 0 && prevProduct > 0) nonSyn = 1;
}

main(int argc, char *argv[]) {

	if ( argc != 6 ) {
		fprintf(stderr,"usage: %s mergedStrainFiles refGenomeFile strainCount polymorphismThreshold unknownsTrheshold\n", argv[0] );
		return -1;
	}
	strainCount = atoi(argv[3]);
	polymorphismThreshold = atoi(argv[4]);
	unknownsThreshold = atoi(argv[5]);

	f1 = fopen(argv[1], "rb");
	if (f1 == 0) {
		fprintf(stderr, "Can't open strainFile '%s' \n", argv[1] );
		return -1;
	}

	f2 = fopen(argv[2], "rb");
	if (f2 == 0) {
		fprintf(stderr,  "Can't open refGenomeFile '%s' \n", argv[2] );
		return -1;
	}

	int f1got;
	int16_t prevSeq;
	int32_t prevLoc;

	// prime things by reading, but not processing, first SNP in the input
	f1got = readStrainRow();
	prevSeq = seq1;
	prevLoc = loc1;
	if (p1 > 0) prevProduct = p1;

	while (seq1 == prevSeq && loc1 == prevLoc && f1got != 0) {
		updateCounts();
		f1got = readStrainRow();		
	}

	// read and process all the rest of the SNPs in the input
	while(f1got != 0) {

		// first process prev SNP and clear counts
		//fprintf(stderr, "%i %i %i %i\n", seq1, prevSeq, loc1, prevLoc);
		if (seq1 != prevSeq || loc1 != prevLoc) processPreviousSnp(prevSeq, prevLoc);

		// update counts with this variant
		updateCounts();

		// remember this variant as previous
		prevSeq = seq1;
		prevLoc = loc1;
		if (prevProduct > 0) prevProduct = p1;

		// read next variant
		f1got = readStrainRow();
	}	
	processPreviousSnp(prevSeq, prevLoc); // process final snp

	fclose(f1);
	fclose(f2);
	return 0;
}

/* 
 * Look at the data accumulated for a SNP.  If above threshold write it out.
 * As part of this, read the refGenome file to convert absent variants to ref genome values.
 */
processPreviousSnp(int32_t prevSeq, int32_t prevLoc) {

	// only consider SNPs that are under unknowns threshold
	if (U_count <= unknownsThreshold) {

		// get reference genome allele and product for this SNP
		getRefGenomeInfo(prevSeq, prevLoc);
		int ref_count = strainCount - sumCount; // sum_count includes unknowns

		if (ref_count > 0 && refProduct != prevProduct && prevProduct > 0) nonSyn = 1;  // we saw some ref alleles, might have a second product

		// find major allele
		int *majorCount;
		majorCount =  &a_count;
		if (c_count > *majorCount) majorCount = &c_count;
		if (g_count > *majorCount) majorCount = &g_count;
		if (t_count > *majorCount) majorCount = &t_count;
		if (ref_count > *majorCount) majorCount = &ref_count;

		// write it out if has enough polymorphisms
		int polyMorphisms = strainCount - U_count - *majorCount;

		if (polyMorphisms >= polymorphismThreshold) {
			printf("%i\t%i\t%i\t%i\t%i\n", prevSeq, prevLoc, U_count, polyMorphisms, nonSyn);
		}
	}

	// zero out counts
	a_count = 0;
	c_count = 0;
	g_count = 0;
	t_count = 0;
	U_count = 0;
	sumCount = 0;
	nonSyn = 0;
}


