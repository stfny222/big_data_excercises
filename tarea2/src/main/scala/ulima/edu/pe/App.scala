package ulima.edu.pe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Formación: 4-4-2 (1 arquero, 4 defensas, 4 mediocampistas y 2 delanteros).
 */
/**
 * Tener 23 años o menos
 */
/**
 * Según la posición en la que se desempeñe, debe tener los mejores skills
 * (de los que ustedes consideren los más importantes para cada posición).
*/
/**
 * Tomar en cuenta su posición preferida (Preferred_Position).
*/
/**
 * No hay inconveniente con el precio del jugador
 * (aún el club tiene caja para realizar las contrataciones).
*/
object App {

  def main(args : Array[String]) {
    // Inicializacion
    val conf = new SparkConf().setAppName("FootballTeam").setMaster("local")
    val sc = new SparkContext(conf)

    // Leer archivo
    val file = sc.textFile("data/FullData.csv")

    // Posociones de la data que se tomarán en cuenta:
    // 0. Name *
    // 6. Club_Kit ** (Precio)
    // 9. Rating **
    // 14. Age **
    // 15. Preffered_Position **
    // 25. Reactions **
    // 27. Interceptions **
    // 38. Agility **
    // 52. GK_Reflexes **

    // Aplicar transformaciones
    val filtered = file.map(x => x.split(","))
                // Solo tomar en cuenta a los menores de 24 años
                .filter(x => x(14).toInt <= 23)
                // Guardar toda la línea como value y Preffered_Position como Key
                .map(x => ( x(15), x ))
    // Agrupar los datos por Preffered_Position
    val rdd = filtered.groupByKey()

    // Actions
    // Verificar que el key de cada grupo sea igual a la posición buscada
    // [Se tomó esta decisión para evitar hacer demasiadas transformaciones por cada tipo de jugador a buscar]
    // Dentro de cada grupo, se realizó un map al segundo valor de la tupla (la línea completa)
    // Se guardó como key el criterio a tomar por la posición del jugador
    // Se guardó como value una tupla del jugador y su precio en el Club
    // Finalmente se hizo un ordenamiento descendente por el key y se eligió a los 2 mejores
    rdd.foreach(player => {
      // Seleccionando Arqueros
      if (player._1.equalsIgnoreCase("GK")) {
        val byGKReflexes = player._2.map(x => ( x(52), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Arqueros con mejor puntuación en GK_Reflexes")
        sorted.foreach(println)
      }
      // Seleccionando Defensas
      if (player._1.equalsIgnoreCase("CB")) {
        val byGKReflexes = player._2.map(x => ( x(25), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Back centrales con mejor puntuación en Reactions")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("LCB/RCB")) {
        val byGKReflexes = player._2.map(x => ( x(25), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Defensas izquierdos / derechos con mejor puntuación en Reactions")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("LB/RB")) {
        val byGKReflexes = player._2.map(x => ( x(25), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Centrales izquierdos / derechos con mejor puntuación en Reactions")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("LWB/RWB")) {
        val byGKReflexes = player._2.map(x => ( x(25), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Laterales izquierdos / derechos con mejor puntuación en Reactions")
        sorted.foreach(println)
      }
      // Seleccionando Mediocampistas
      if (player._1.equalsIgnoreCase("CDM")) {
        val byGKReflexes = player._2.map(x => ( x(27), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Mediocampistas centrales defensivos con mejor puntuación en Interceptions")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("CM")) {
        val byGKReflexes = player._2.map(x => ( x(27), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Mediocampistas centrales con mejor puntuación en Interceptions")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("CAM")) {
        val byGKReflexes = player._2.map(x => ( x(38), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Mediocampistas centrales de ataque con mejor puntuación en Agility")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("LAM/RAM")) {
        val byGKReflexes = player._2.map(x => ( x(38), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Mediocampistas izquierdos / derechos de ataque con mejor puntuación en Agility")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("LCM/RCM")) {
        val byGKReflexes = player._2.map(x => ( x(27), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Mediocampistas centrales izquierdos / derechos con mejor puntuación en Interceptions")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("LDM/RDM")) {
        val byGKReflexes = player._2.map(x => ( x(27), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Mediocampistas defensivos izquierdos / derechos con mejor puntuación en Interceptions")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("LM/RM")) {
        val byGKReflexes = player._2.map(x => ( x(27), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Mediocampistas izquierdo / derecho con mejor puntuación en Interceptions")
        sorted.foreach(println)
      }
      // Seleccionando Delanteros
      if (player._1.equalsIgnoreCase("CF")) {
        val byGKReflexes = player._2.map(x => ( x(9), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Centrodelanteros con mejor Rating")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("LF/RF")) {
        val byGKReflexes = player._2.map(x => ( x(9), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Delanteros izquierdo / derecho (aleros)")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("LS/RS")) {
        val byGKReflexes = player._2.map(x => ( x(9), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Delanteros izquierdo / derecho con mejor Rating")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("LW/RW")) {
        val byGKReflexes = player._2.map(x => ( x(9), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Wing izquierdo / derecho con mejor Rating")
        sorted.foreach(println)
      }
      if (player._1.equalsIgnoreCase("ST")) {
        val byGKReflexes = player._2.map(x => ( x(9), (x(0), x(6))))
        val sorted = byGKReflexes.toList.sortWith(_._1 > _._1)
                .take(2)
        println("Delanteros con mejor Rating")
        sorted.foreach(println)
      }
    })
  }

}
