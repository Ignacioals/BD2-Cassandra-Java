package ar.com.tp.cassandra;

import java.util.ArrayList;
import java.util.Hashtable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class Metodos {

  private static CqlSession session;

  public void connect() {

    session = CqlSession.builder().build();

    System.out.printf("Connected session: %s%n", session.getName());
  }

  public void close() {
    if (session != null) {
      session.close();
    }
  }

  /*Consulta 8*/
  public void albumesConDeterminadoTema(int id_cancion) {
		ResultSet albumes =
				session.execute("select id_album, nombre_album, ano_album, portada_album from tp_cassandra.tabla578 where id_cancion = "+id_cancion+" allow filtering;");
		for(Row r: albumes) {
			System.out.println("Id album: "+ r.getInt("id_album"));
			  System.out.println("Nombre Album: "+ r.getString("nombre_album") );
			  System.out.println("Año album: "+ r.getInt("ano_album"));
			  System.out.println("---------------");
			
		}
		
  }
  
  
  
  /* Consulta 4*/
  public void musicoConMasCancionesInterpretadas() {
	  ResultSet max =
			  session.execute(
					  "SELECT max(cant_canciones) FROM tp_cassandra.tablaMetod4;"
					  );
	  long maxcant = max.one().getLong(0); 
	  ResultSet interpretes =
			  session.execute(
					  "SELECT * from tp_cassandra.tablaMetod4;");
	  ArrayList<Integer> ids_interpretes = new ArrayList<Integer>();
	  for(Row r : interpretes) {
		  if(r.getLong("cant_canciones") == maxcant) {
			  ids_interpretes.add(r.getInt("id_banda"));
			  }
	  }
	  for(int i : ids_interpretes) {
		  ResultSet musico = 
				  session.execute("SELECT id_interprete_cancion, nombre_banda, apellido_banda "
				  		+ "from tp_cassandra.tabla234 where id_interprete_cancion = "+i+" LIMIT 1 ALLOW FILTERING ;");
		  for(Row r : musico){
		  System.out.println("Id musico: "+r.getInt("id_interprete_cancion"));
		  System.out.println("Nombre banda: "+r.getString("nombre_banda"));
		  System.out.println("Apellido banda: "+r.getString("apellido_banda"));
		  System.out.println("----------------");
		  }
	  }

  }
  /*Consulta 6*/
  public void resenasDelAlbumQueContienCancion(int idcancion) {
	  ResultSet resena =
			  session.execute("select id_resena, id_autor_resena, apellido_autor_resena,nombre_autor_resena, fecha_resena, texto_resena "
			  		+ "from tp_cassandra.tabla6 where id_canciones contains " + idcancion +";");
	  for (Row r: resena) {
		  System.out.println("Id reseña: "+ r.getInt("id_resena"));
		  System.out.println("Id autor reseña: "+ r.getInt("id_autor_resena") );
		  System.out.println("Nombre y apellido: "+ r.getString("nombre_autor_resena") + " " +r.getString("apellido_autor_resena"));
		  System.out.println("Reseña: \n \t" + r.getString("texto_resena"));
		  System.out.println("---------------");
	  }
  }
  
  public int agregarResenaAlbum(int id_album, String texto_resena, int id_usr) {
	  int id_resena = (int) session.execute("select count(*) from tp_cassandra.tabla6").one().getLong(0) +1;

	  Hashtable<String, String> info_album = getAlbum(id_album);
	  ResultSet usuario = session.execute("select id_usuario, nombre_usuario, apellido_usuario from tp_cassandra.tabla1 where id_usuario = " +id_usr+" limit 1;");
	  for (Row r : usuario) {
	  session.execute("INSERT INTO tp_cassandra.tabla6 (id_album, nombre_album, id_autor_resena, nombre_autor_resena, apellido_autor_resena,id_resena, texto_resena, fecha_resena, id_canciones)"
	  		+ " VALUES ("
	  		+ info_album.get("id_album")+ " ,"
	  		+ "'" +info_album.get("nombre_album")+ "' ,"
	  		+ r.getInt("id_usuario")+ " ,"
	  		+ "'"+r.getString("nombre_usuario")+ "' ,"
	  		+ "'"+r.getString("apellido_usuario") +"' ,"
	  		+ id_resena +" ,"
	  		+ "'"+texto_resena+"' ,"
	  		+ System.currentTimeMillis() + " ,"
	  		+ info_album.get("id_canciones") + ");");
	  System.out.println("La reseña ha sido creada, reseña numero: "+ id_resena);
	  }
	  return id_resena;
  }
  
  public void eliminarResena(int id_resena, int id_album) {
	  Hashtable<String, String> info_album = getAlbum(id_album);
	  session.execute("Delete from tp_cassandra.tabla6 where id_album = "+info_album.get("id_album") +" AND nombre_album = '"+ info_album.get("nombre_album")+"' AND id_resena = "+id_resena +";");
  }
  
  private Hashtable<String,String> getAlbum(int id_album) {
	Hashtable<String, String> res = new Hashtable<String, String>();
	String id_canciones = "[";
	boolean primero = true;
	ResultSet canciones = 
			session.execute("select id_cancion, id_album, nombre_album from tp_cassandra.tabla578 where id_album = "+id_album+" ALLOW FILTERING;");
	for(Row r : canciones) {
		if(!primero) {
			id_canciones = id_canciones.concat(", ");
		}else {
			res.put("id_album", ""+r.getInt("id_album"));
			res.put("nombre_album", r.getString("nombre_album"));
			primero = !primero;
		}
		id_canciones= id_canciones.concat(""+r.getInt("id_cancion"));
	}
	id_canciones = id_canciones.concat("]");
	res.put("id_canciones", id_canciones);

	return res;
}
		




}