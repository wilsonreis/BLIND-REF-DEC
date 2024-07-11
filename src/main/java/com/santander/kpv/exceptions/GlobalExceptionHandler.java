package com.santander.kpv.exceptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import org.springframework.dao.QueryTimeoutException;

import jakarta.servlet.http.HttpServletRequest;


@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

	private static final Logger logger_ = Logger.getLogger(GlobalExceptionHandler.class.getName());

	@Autowired
	private MessageSource messageSource;
	
	  @ExceptionHandler(QueryTimeoutException.class)
	  public ResponseEntity<Object> handleQueryTimeoutException(HttpServletRequest req, Exception ex) {
	    logger.error("Request: " + req.getRequestURL() + " levantada " + ex);
		List<Erro> erros = new ArrayList<>();
		erros.add(new Erro("Excessão imprevista (QueryTimeoutException)", ex.getLocalizedMessage()));
		return handleExceptionInternal(null, erros, new HttpHeaders(), HttpStatus.BAD_REQUEST, null);
	  }

	  @ExceptionHandler(MyRuntimeException.class)
	  public ResponseEntity<Object> handleError(HttpServletRequest req, Exception ex) {
	    logger.error("Request: " + req.getRequestURL() + " levantada " + ex);
		List<Erro> erros = new ArrayList<>();
		erros.add(new Erro("Excessão imprevista/Gateway Timeout (Exception)", ex.getLocalizedMessage()));
		return handleExceptionInternal(null, erros, new HttpHeaders(), HttpStatus.GATEWAY_TIMEOUT, null);
	  }

	@Override
	protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
			HttpHeaders headers, HttpStatusCode status, WebRequest request) {
		logger_.log(Level.INFO, "handleMethodArgumentNotValid : {0} ", ex.getLocalizedMessage() + "----" + ex.getMessage());
		String mensagemDesenvolvedor = ExceptionUtils.getRootCauseMessage(ex);
		List<Erro> erros = criarListaDeErros(ex.getBindingResult());
		return handleExceptionInternal(ex, erros, new HttpHeaders(), HttpStatus.BAD_REQUEST, request);		

	}

	@ExceptionHandler({ EmptyResultDataAccessException.class })
	public ResponseEntity<Object> handleEmptyResultDataAccessException(EmptyResultDataAccessException ex,
			WebRequest request) {
		logger_.log(Level.INFO, "handleEmptyResultDataAccessException : {0} ", ex.getLocalizedMessage());
		Locale bLocale = extrairIdioma(request);
		String mensagemUsuario = messageSource.getMessage("recurso.nao-encontrado", null,
				bLocale);
		String mensagemDesenvolvedor = ex.toString();
		List<Erro> erros = Arrays.asList(new Erro(mensagemUsuario, mensagemDesenvolvedor));
		return handleExceptionInternal(ex, erros, new HttpHeaders(), HttpStatus.NOT_FOUND, request);
	}
	
	@ExceptionHandler({ DataIntegrityViolationException.class })
	public ResponseEntity<Object> handleDataIntegrityViolationException(DataIntegrityViolationException ex,
			WebRequest request) {
		logger_.log(Level.INFO, "handleDataIntegrityViolationException : {0} ", ex.getLocalizedMessage());
		Locale bLocale = extrairIdioma(request);
		
		String mensagemUsuario = null;
		if (ExceptionUtils.getRootCauseMessage(ex).contains("already exists") || ExceptionUtils.getRootCauseMessage(ex).contains("duplicate key")) {
			mensagemUsuario = messageSource.getMessage("registro.duplicado", null, bLocale);
		} else {
			if (ExceptionUtils.getRootCauseMessage(ex).contains("not-null")) {
				mensagemUsuario = messageSource.getMessage("verifique.campo.nulo", null, bLocale);
			} else {
				mensagemUsuario = messageSource.getMessage("recurso.operacao-nao-permitida", null,
						bLocale);				
			}
		}
		String mensagemDesenvolvedor = ExceptionUtils.getRootCauseMessage(ex);
		List<Erro> erros = Arrays.asList(new Erro(mensagemUsuario, mensagemDesenvolvedor));
		return handleExceptionInternal(ex, erros, new HttpHeaders(), HttpStatus.BAD_REQUEST, request);
	}

	private Locale extrairIdioma(WebRequest request) {
		Locale bLocale = null;
		try {
			if (Locale.LanguageRange.parse(request.getHeader("Accept-Language")).get(0).toString().equals("en-us")) {			
				 bLocale = new Locale.Builder().setLanguage("en").setRegion("US").build();
			} else {
				bLocale = new Locale.Builder().setLanguage("pt").setRegion("BR").build();
			}			
		} catch (Exception e) {
			bLocale = new Locale.Builder().setLanguage("pt").setRegion("BR").build();
		}
		return bLocale;
	}

	private List<Erro> criarListaDeErros(BindingResult bindingResult) {
		logger_.log(Level.INFO, "criarListaDeErros : {0} erros", bindingResult.getFieldErrorCount());
		List<Erro> erros = new ArrayList<>();

		for (FieldError fieldError : bindingResult.getFieldErrors()) {
			String mensagemUsuario = messageSource.getMessage(fieldError, LocaleContextHolder.getLocale());
			String mensagemUsuarioTratada = null;
			try {
				mensagemUsuarioTratada = messageSource.getMessage(mensagemUsuario, null,
						LocaleContextHolder.getLocale());
			} catch (Exception e) {
				mensagemUsuarioTratada = mensagemUsuario;
			}

			String mensagemDesenvolvedor = fieldError.toString();
			erros.add(new Erro(mensagemUsuarioTratada, mensagemDesenvolvedor));
		}

		return erros;
	}

	public static class Erro {

		private String mensagemUsuario;
		private String mensagemDesenvolvedor;

		public Erro(String mensagemUsuario, String mensagemDesenvolvedor) {
			this.mensagemUsuario = mensagemUsuario;
			this.mensagemDesenvolvedor = mensagemDesenvolvedor;
		}

		public String getMensagemUsuario() {
			return mensagemUsuario;
		}

		public String getMensagemDesenvolvedor() {
			return mensagemDesenvolvedor;
		}

	}

}
