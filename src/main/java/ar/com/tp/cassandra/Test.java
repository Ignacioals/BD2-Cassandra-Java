package ar.com.tp.cassandra;

import ar.com.tp.cassandra.Metodos;

public class Test {
	  public static void main(String[] args) {

		    Metodos client = new Metodos();
		    
		    try {
		    	client.connect();
		    	System.out.println("Ejemplos de lectura de la base de datos");
		    	System.out.println("8.	Los álbumes que tienen un determinado tema");
		    	client.albumesConDeterminadoTema(12);
		    	System.out.println("4.	Que músico tiene más temas interpretados");
		    	client.musicoConMasCancionesInterpretadas();
		    	System.out.println("6.	Reseñas de los álbumes que tiene una canción.");
		    	client.resenasDelAlbumQueContienCancion(12);
		    	System.out.println("Create y Delete De reseñas");
		    	int ultima_resena = client.agregarResenaAlbum(1,"Me gusto", 1);
		    	System.out.println("Justificacion: ");
		    	client.resenasDelAlbumQueContienCancion(2);
		    	client.eliminarResena(ultima_resena, 1);
		    	System.out.println("Justificacion de que se eliminó: ");
		    	client.resenasDelAlbumQueContienCancion(2);
		    	client.close();
		    } catch (Exception ex) {
		      ex.printStackTrace();
		    } finally {
		      client.close();
		    }
		  }

}
