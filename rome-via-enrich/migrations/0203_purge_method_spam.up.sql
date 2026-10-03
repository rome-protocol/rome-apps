-- Purge 4byte collision spam + "unknown()" placeholder so the decoder revisits
-- these selectors with the new spam filter and raw-selector fallback.
DELETE FROM rome_via.method_signatures
WHERE signature LIKE '\_SIMONdotBLACK\_%' ESCAPE '\'
   OR signature = 'unknown()';
