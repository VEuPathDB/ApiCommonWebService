#include <stdio.h>
#include <stdint.h>
#include <string.h> 

// using global variables to cut down on stack pushing operations.
FILE *strainFile;
FILE *refFile;
int polymorphismThreshold;
int unknownsThreshold;
int strainCount;
char *sourceIdPrefix;

// input strains
int16_t seq = -1;
int16_t *seq_p = &seq;
int32_t loc = -1;
int32_t *loc_p = &loc;
char allele = -1;  
char *allele_p = &allele;
char product = -1;  
char *product_p = &product;
int8_t strain = -1;  
int8_t *strain_p = &strain;

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
int diploidCount = 0; // for a given SNP, the number of variants that are diploid.  incremented for each variant beyond the first per strain.
int sumCount = 0;

int prevProduct = -1;
int prevStrain = -1;
int nonSyn = 0;
int16_t prevSeq;
int32_t prevLoc;


static inline int readStrainRow() {
	prevSeq = seq;
	prevLoc = loc;
	if (product > 0) prevProduct = product;  // remember last known product
	prevStrain = strain;

	fread(seq_p, 2, 1, strainFile);  
	fread(loc_p, 4, 1, strainFile);  
	fread(allele_p, 1, 1, strainFile); 
	fread(product_p, 1, 1, strainFile);
	return fread(strain_p, 2, 1, strainFile);
}

static inline getRefGenomeInfo(int16_t seq, int32_t loc) {
	// refGenome file is a strain file: one row per SNP, showing the ref genomes values
	// advance through SNPs to our current one
	while(refSeq != seq && refLoc != loc) {
		fread(refSeq_p, 2, 1, refFile);  
		fread(refLoc_p, 4, 1, refFile);  
		fread(refAllele_p, 1, 1, refFile); 
		fread(refProduct_p, 1, 1, refFile);
	}
}

static inline updateCounts() {
	if (allele == 0 && product == -1) {
		U_count += strain;
		sumCount += strain;
	} else {
		if (allele == 1) a_count++;
		else if (allele == 2) c_count++;
		else if (allele == 3) g_count++;
		else if (allele == 4) t_count++;
		else U_count++;
		sumCount++;
		if (strain == prevStrain) diploidCount++;  // more than one variant for this strain.  count as a diploid variant
	}
	if (product != prevProduct && product > 0 && prevProduct > 0) nonSyn = 1;
}

main(int argc, char *argv[]) {

	if ( argc != 6 ) {
		fprintf(stderr,"usage: %s mergedStrainFiles refGenomeFile strainCount polymorphismThreshold unknownsTrheshold\n", argv[0] );
		return -1;
	}
	strainCount = atoi(argv[3]);
	polymorphismThreshold = atoi(argv[4]);
	unknownsThreshold = atoi(argv[5]);

	strainFile = fopen(argv[1], "rb");
	if (strainFile == 0) {
		fprintf(stderr, "Can't open strainFile '%s' \n", argv[1] );
		return -1;
	}

	refFile = fopen(argv[2], "rb");
	if (refFile == 0) {
		fprintf(stderr,  "Can't open refGenomeFile '%s' \n", argv[2] );
		return -1;
	}

	int strainFileGot;

	// prime things by reading, but not processing, first SNP in the input
	strainFileGot = readStrainRow();
	prevSeq = seq;
	prevLoc = loc;
	while (seq == prevSeq && loc == prevLoc && strainFileGot != 0) {
		updateCounts();
		strainFileGot = readStrainRow();		
	}

	// read and process the rest of the SNPs in the input
	while(strainFileGot != 0) {

		// first process prev SNP and clear counts
		// fprintf(stderr, "%i %i %i %i\n", seq, prevSeq, loc, prevLoc);
		if (seq != prevSeq || loc != prevLoc) processPreviousSnp(prevSeq, prevLoc);

		// update counts with this variant
		updateCounts();

		// read next variant
		strainFileGot = readStrainRow();
	}	
	processPreviousSnp(prevSeq, prevLoc); // process final snp

	fclose(strainFile);
	fclose(refFile);
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

		int ref_count = (strainCount + diploidCount) - sumCount; // sumCount includes unknowns

		if (ref_count > 0 && refProduct != prevProduct && prevProduct > 0) nonSyn = 1;  // we saw some ref alleles, might have a second product

		// add in ref allele
		if (refAllele == 1) a_count += ref_count;
		else if (refAllele == 2) c_count += ref_count;
		else if (refAllele == 3) g_count += ref_count;
		else if (refAllele == 4) t_count += ref_count;

		// find major allele
		int *majorCount;
		majorCount =  &a_count;
		if (c_count > *majorCount) majorCount = &c_count;
		if (g_count > *majorCount) majorCount = &g_count;
		if (t_count > *majorCount) majorCount = &t_count;

		// write it out if has enough polymorphisms
		int polymorphisms = strainCount - U_count - *majorCount;
		int polymorphismsPercent = (polymorphisms * 100) / (strainCount - U_count);
		int knownPercent = (strainCount - U_count) * 100 / strainCount;

		if (polymorphisms >= polymorphismThreshold) {
			printf("%i\t%i\t%i\t%i\t%i\n", prevSeq, prevLoc, knownPercent, polymorphismsPercent, nonSyn);
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
	diploidCount = 0;

	// reset prev for new snp
	prevProduct = -1;
	prevStrain = -1;
}


