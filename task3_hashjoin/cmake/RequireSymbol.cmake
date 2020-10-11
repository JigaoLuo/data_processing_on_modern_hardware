include(CheckCXXSymbolExists)

function(require_symbol SYMBOL)
   check_cxx_symbol_exists("${SYMBOL}" "${ARGN}" "HAVE_${SYMBOL}")

   if (NOT HAVE_${SYMBOL})
      message(FATAL_ERROR "${SYMBOL} is required (${HAVE_${SYMBOL}})")
   endif ()
endfunction()