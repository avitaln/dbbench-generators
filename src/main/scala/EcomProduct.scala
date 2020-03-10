import java.util.UUID

import scala.util.Random._

case class OptionSelection(
                            value: String
                          )

case class ProductOption(
                          optionId: String,
                          title: String,
                          selections: Seq[OptionSelection]
                        )
case class EcomProduct(
                        productId: String,
                        name: String,
                        price: BigDecimal,
                        description: String = "",
                        collections: Seq[String] = Nil,
                        options: Seq[ProductOption] = Nil
                      )

object ProductGenerator extends App {

  lazy val adjs = List("autumn", "hidden", "bitter", "misty", "silent",
    "empty", "dry", "dark", "summer", "icy", "delicate", "quiet", "white", "cool",
    "spring", "winter", "patient", "twilight", "dawn", "crimson", "wispy",
    "weathered", "blue", "billowing", "broken", "cold", "damp", "falling",
    "frosty", "green", "long", "late", "lingering", "bold", "little", "morning",
    "muddy", "old", "red", "rough", "still", "small", "sparkling", "throbbing",
    "shy", "wandering", "withered", "wild", "black", "holy", "solitary",
    "fragrant", "aged", "snowy", "proud", "floral", "restless", "divine",
    "polished", "purple", "lively", "nameless", "puffy", "fluffy",
    "calm", "young", "golden", "avenging", "ancestral", "ancient", "argent",
    "reckless", "daunting", "short", "rising", "strong", "timber", "tumbling",
    "silver", "dusty", "celestial", "cosmic", "crescent", "double", "far", "half",
    "inner", "milky", "northern", "southern", "eastern", "western", "outer",
    "terrestrial", "huge", "deep", "epic", "titanic", "mighty", "powerful")

  lazy val nouns = List("waterfall", "river", "breeze", "moon", "rain",
    "wind", "sea", "morning", "snow", "lake", "sunset", "pine", "shadow", "leaf",
    "dawn", "glitter", "forest", "hill", "cloud", "meadow", "glade",
    "bird", "brook", "butterfly", "bush", "dew", "dust", "field",
    "flower", "firefly", "feather", "grass", "haze", "mountain", "night", "pond",
    "darkness", "snowflake", "silence", "sound", "sky", "shape", "surf",
    "thunder", "violet", "wildflower", "wave", "water", "resonance",
    "sun", "wood", "dream", "cherry", "tree", "fog", "frost", "voice", "paper",
    "frog", "smoke", "star", "sierra", "castle", "fortress", "tiger", "day",
    "sequoia", "cedar", "wrath", "blessing", "spirit", "nova", "storm", "burst",
    "protector", "drake", "dragon", "knight", "fire", "king", "jungle", "queen",
    "giant", "elemental", "throne", "game", "weed", "stone", "apogee", "bang",
    "cluster", "corona", "cosmos", "equinox", "horizon", "light", "nebula",
    "solstice", "spectrum", "universe", "magnitude", "parallax")

  lazy val allWords = adjs ++ nouns

  lazy val sizes1 = Seq("small", "medium", "large")
  lazy val sizes2 = Seq("small", "medium", "large", "xxlarge")
  lazy val sizes3 = Seq("small", "medium", "large", "xxlarge", "xxlarge")
  lazy val colors1 = Seq("black", "white", "red", "green", "blue")
  lazy val colors2 = Seq("black", "white", "red", "green", "yellow", "purple")
  lazy val colors3 = Seq("black", "white")

  lazy val optionsIds = Seq(UUID.randomUUID().toString, UUID.randomUUID().toString)
  lazy val optionTitles = Seq("Size", "Color")
  lazy val choices = Seq(
    Seq(sizes1, sizes2, sizes3),
    Seq(colors1, colors3, colors3)
  )


  def getRandElt(xs: List[String]): String = xs(nextInt(xs.size))

  def getRandNumber(ra: Range): String = {
    (ra.head + nextInt(ra.end - ra.head)).toString
  }

  def genName() = s"${getRandElt(adjs)} ${getRandElt(nouns)}"

  def genPrice(): BigDecimal = {
    val steps = (120-8)*10
    val offset = nextInt(steps)
    BigDecimal(80 + offset) * BigDecimal("0.1")
  }

  def genDescription(): String = {
    val numOfWords = nextInt(200) + 10
    List.fill(numOfWords)(getRandElt(allWords)).mkString(" ")
  }

  def genCollections(): Seq[String] = {
    (1 to 20).flatMap { i =>if (nextBoolean()) Some(s"cat$i") else None }
  }


  def genOption(i: Int): ProductOption = {
    val j = nextInt(3)
    ProductOption(
      optionId = optionsIds(i),
      title = optionTitles(i),
      selections = choices(i)(j).map(OptionSelection)
    )
  }

  def genOptions(): Seq[ProductOption] = {
    (0 to 1).flatMap { i =>
      if (nextBoolean()) {
        val po: ProductOption = genOption(i)
        Some(po)
      } else {
        None
      }
    }
  }

  def gen: EcomProduct = {
    EcomProduct(
      productId = UUID.randomUUID().toString,
      name = genName(),
      price = genPrice(),
      description = genDescription(),
      collections = genCollections(),
      options = genOptions()
    )
  }
}