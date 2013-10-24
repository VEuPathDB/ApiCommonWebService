#include <stdio.h>
#include <stdint.h>
#include <string.h>

inline static int readStrainRow(int16_t *seq, int32_t *loc, int8_t *allele, char *product, int16_t *strain, int16_t cmdLineStrain) {
	fread(seq, 2, 1, stdin);  
	fread(loc, 4, 1, stdin);  
	fread(allele, 1, 1, stdin); 
	int retval = fread(product, 1, 1, stdin);
	if (cmdLineStrain == 0) retval = fread(strain, 2, 1, stdin);
	else *strain = cmdLineStrain;
	return retval;
}

main(int argc, char *argv[]) {
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
	int16_t cmdLineStrain1;

	if ( argc != 2 ) {
		printf( "usage: %s strain_id < strainFile\n\nstrain_id should be 0 if input includes strain info per row \n\n", argv[0] );
		return -1;
	}
	cmdLineStrain1 = atoi(argv[1]);

	int f1got;
	f1got = readStrainRow(seq1_p, loc1_p, a1_p, p1_p, strain1_p, cmdLineStrain1);

	while(f1got != 0) {
		printf("%i\t%i\t%i\t%i\t%i\n", seq1, loc1, a1, p1, strain1);
		f1got = readStrainRow(seq1_p, loc1_p, a1_p, p1_p, strain1_p, cmdLineStrain1);
	}
	return 0;
}

