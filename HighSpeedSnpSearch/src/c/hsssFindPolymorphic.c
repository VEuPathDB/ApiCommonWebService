#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h> 

// using global variables to cut down on stack pushing operations.
FILE *strainFile;
FILE *refFile;
int polymorphismThreshold;
int unknownsThreshold;
int strainCount;
int alleleCount;
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
int nonRefStrainsCount = 0;

int prevProduct = -1;
int prevStrain = -1;
int nonSyn = 0;
int16_t prevSeq;
int32_t prevLoc;

static inline int freadCheck (char *filename, void *ptr, size_t size, size_t count, FILE *stream) {
	int bytes = fread(ptr, size, count,stream);
	if (ferror(stream)) {
		fprintf(stderr, "Failed reading file '%s' \n", filename );
	 	exit(-1);
	}
	return bytes;
}


static inline int readStrainRow(char *filename) {
	prevSeq = seq;
	prevLoc = loc;
	if (product > 0) prevProduct = product;  // remember last known product
	prevStrain = strain;

	freadCheck(filename, seq_p, 2, 1, strainFile);  
	freadCheck(filename, loc_p, 4, 1, strainFile);  
	freadCheck(filename, allele_p, 1, 1, strainFile); 
	freadCheck(filename, product_p, 1, 1, strainFile);
	return freadCheck(filename, strain_p, 2, 1, strainFile);
}

static inline getRefGenomeInfo(char *filename, int16_t seq, int32_t loc) {
	// refGenome file is a strain file: one row per SNP, showing the ref genomes values
	// advance through SNPs to our current one
	//	fprintf(stderr, "getRef %i %i %i %i\n", seq, loc, refSeq, refLoc);
		while(!(refSeq == seq && refLoc== loc)) {
		freadCheck(filename, refSeq_p, 2, 1, refFile);  
		freadCheck(filename, refLoc_p, 4, 1, refFile);  
		freadCheck(filename, refAllele_p, 1, 1, refFile); 
		freadCheck(filename, refProduct_p, 1, 1, refFile);
		//fprintf(stderr,"%i %i\n" ,refSeq, refLoc);
	}
}

static inline updateCounts() {
	if (allele == 0 && product == -1) {
		U_count += strain;
		nonRefStrainsCount += strain;
	} else {
		if (allele == 1) { a_count++; alleleCount++; }
		else if (allele == 2) { c_count++; alleleCount++; }
		else if (allele == 3) { g_count++; alleleCount++; }
		else if (allele == 4) { t_count++; alleleCount++; }
		else U_count++;
		if (strain != prevStrain) nonRefStrainsCount++;
	}
	if (product != prevProduct && product > 0 && prevProduct > 0) nonSyn = 1;
}

main(int argc, char *argv[]) {

	if ( argc != 6 ) {
		fprintf(stderr,"\nUsage: %s mergedStrainFiles refGenomeFile strainCount polymorphismThreshold unknownsThreshold\n\nWhere:\n  strainCount: number of input strains\n  polymorphismThreshold:  there must be this many or more non-major alleles for a SNP to be reported\n  unknownsThreshold: there must be this many or fewer unknowns for this SNP to be reported.\n\n", argv[0] );
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
	strainFileGot = readStrainRow(argv[1]);
	prevSeq = seq;
	prevLoc = loc;
	while (seq == prevSeq && loc == prevLoc && strainFileGot != 0) {
		updateCounts();
		strainFileGot = readStrainRow(argv[1]);		
	}

	// read and process the rest of the SNPs in the input
	while(strainFileGot != 0) {

		// first process prev SNP and clear counts
		// fprintf(stderr, "%i %i %i %i\n", seq, prevSeq, loc, prevLoc);
		if (seq != prevSeq || loc != prevLoc) processPreviousSnp(prevSeq, prevLoc, argv[2]);

		// update counts with this variant
		updateCounts();

		// read next variant
		strainFileGot = readStrainRow(argv[1]);
	}	
	processPreviousSnp(prevSeq, prevLoc, argv[2]); // process final snp

	fclose(strainFile);
	fclose(refFile);
	return 0;
}

/* 
 * Look at the data accumulated for a SNP.  If above threshold write it out.
 * As part of this, read the refGenome file to convert absent variants to ref genome values.
 */
	processPreviousSnp(int32_t prevSeq, int32_t prevLoc, char *refGenomeFileName) {

	// only consider SNPs that are under or equal to unknowns threshold
	if (U_count <= unknownsThreshold) {

		// get reference genome allele and product for this SNP
		getRefGenomeInfo(refGenomeFileName, prevSeq, prevLoc);

		int ref_count = strainCount - nonRefStrainsCount; // nonRefStrainsCount includes unknowns; diploid strains are only counted once

		alleleCount += ref_count;

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

		// fprintf(stderr, "seq:%i loc:%i refSeq:%i refLoc:%i refAllele:%i ref_count:%i, a:%i c:%i g:%i t:%i nonRefStrainsCount:%i\n", prevSeq, prevLoc, refSeq, refLoc, refAllele, ref_count, a_count, c_count, g_count, t_count,nonRefStrainsCount );

		// write it out if has enough polymorphisms
		int polymorphisms = alleleCount - *majorCount;
		int polymorphismsPercent = (polymorphisms * 100) / alleleCount;
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
	nonRefStrainsCount = 0;
	nonSyn = 0;
	diploidCount = 0;
	alleleCount = 0;

	// reset prev for new snp
	prevProduct = -1;
	prevStrain = -1;
}


