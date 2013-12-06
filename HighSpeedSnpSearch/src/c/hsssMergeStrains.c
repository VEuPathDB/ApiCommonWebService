#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static inline int freadCheck (char *filename, void *ptr, size_t size, size_t count, FILE *stream) {
	int bytes = fread(ptr, size, count,stream);
	if (ferror(stream)) {
		fprintf(stderr, "Failed reading file '%s' \n", filename );
	 	exit(-1);
	}
	return bytes;
}

static inline int fwriteCheck (char *filename, void *ptr, size_t size, size_t count, FILE *stream) {
	fwrite(ptr, size, count,stream);
	if (ferror(stream)) {
		fprintf(stderr, "Failed writing to file '%s' \n", filename );
	 	exit(-1);
	}
}

// input files that are unmerged strain files have a constant strain; the rows do not include strain.
// in that case the strain is provided on the cmd line.  otherwise read the strain from the file.
inline static int readStrainRow(FILE *file, int16_t *seq, int32_t *loc, int8_t *allele, char *product, int16_t *strain, int16_t cmdLineStrain, char *filename) {
	int retval;
	freadCheck(filename, seq, 2, 1, file);  
	freadCheck(filename, loc, 4, 1, file);  
	freadCheck(filename, allele, 1, 1, file); 
	retval = freadCheck(filename, product, 1, 1, file);
	if (cmdLineStrain == 0) retval = freadCheck(filename, strain, 2, 1, file);
	else *strain = cmdLineStrain;
	return retval;
}

inline static int writeStrainRowAndReadNext(FILE *file, int16_t *seq, int32_t *loc, int8_t *allele, char *product, int16_t *strain, int16_t cmdLineStrain, char *filename) {
	fwriteCheck(filename, seq, 2, 1, stdout);  
	fwriteCheck(filename, loc, 4, 1, stdout);  
	fwriteCheck(filename, allele, 1, 1, stdout); 
	fwriteCheck(filename, product, 1, 1, stdout);
	fwriteCheck(filename, strain, 2, 1, stdout);
	return readStrainRow(file, seq, loc, allele, product, strain, cmdLineStrain, filename);
}

main(int argc, char *argv[]) {
	FILE *f1;
	FILE *f2;
  int16_t cmdLineStrain1;
  int16_t cmdLineStrain2;

  int16_t seq1;
  int16_t *seq1_p = &seq1;
  int32_t loc1;
  int32_t *loc1_p = &loc1;
  char a1;  // allele
  char *a1_p = &a1;
  char p1;  // product
  char *p1_p = &p1;
  int16_t strain1;
  int16_t *strain1_p = &strain1;

  int16_t seq2;
  int16_t *seq2_p = &seq2;
  int32_t loc2;
  int32_t *loc2_p = &loc2;
  char a2;  // allele
  char *a2_p = &a2;
  char p2;  // product
  char *p2_p = &p2;
  int16_t strain2;
  int16_t *strain2_p = &strain2;

	if ( argc != 5 ) {
		fprintf(stderr, "usage: %s strain1_file strain1_id strain2_file strain2_id \n\nstrain1_id should be 0 if strain1_file includes strain info per row (and same for strain2)\n\n", argv[0] );
		return -1;
	}

	f1 = fopen(argv[1], "rb");
	if (f1 == 0) {
		fprintf(stderr, "Can't open file1 '%s' \n", argv[1] );
		return -1;
	}
	cmdLineStrain1 = atoi(argv[2]);

	f2 = fopen(argv[3], "rb");
	if (f2 == 0) {
		fprintf(stderr,"Can't open file2 '%s' \n", argv[2] );
		return -1;
	}
	cmdLineStrain2 = atoi(argv[4]);

	int f1got;
	int f2got;
	f1got = readStrainRow(f1, seq1_p, loc1_p, a1_p, p1_p, strain1_p, cmdLineStrain1, argv[1]);
	f2got = readStrainRow(f2, seq2_p, loc2_p, a2_p, p2_p, strain2_p, cmdLineStrain2, argv[2]);

	while(1 == 1) {
		while ((seq1 < seq2 || (seq1 == seq2 && loc1 < loc2)) && f1got != 0) { 
			f1got = writeStrainRowAndReadNext(f1, seq1_p, loc1_p, a1_p, p1_p, strain1_p, cmdLineStrain1, argv[1]);
		}
		while ((seq2 < seq1 || (seq2 == seq1 && loc2 < loc1)) && f2got != 0) {
			f2got = writeStrainRowAndReadNext(f2, seq2_p, loc2_p, a2_p, p2_p, strain2_p, cmdLineStrain2, argv[2]);
		}
		if (seq1 == seq2 && loc1 == loc2) {
			if (f1got != 0) f1got = writeStrainRowAndReadNext(f1, seq1_p, loc1_p, a1_p, p1_p, strain1_p, cmdLineStrain1, argv[1]);
			if (f2got != 0) f2got = writeStrainRowAndReadNext(f2, seq2_p, loc2_p, a2_p, p2_p, strain2_p, cmdLineStrain2, argv[2]);
		}
		if (f1got == 0 && f2got == 0) break;
		if (f2got == 0) f1got = writeStrainRowAndReadNext(f1, seq1_p, loc1_p, a1_p, p1_p, strain1_p, cmdLineStrain1, argv[1]);
		else if (f1got == 0) f2got = writeStrainRowAndReadNext(f2, seq2_p, loc2_p, a2_p, p2_p, strain2_p, cmdLineStrain2, argv[2]);
	}
	fclose(f1);
	fclose(f2);
	return 0;
}


