using System;
using System.Diagnostics;
using System.IO;

namespace OUFLib.Helpers
{
    public class ZipHelper
    {
        private const int ZIP_LEAD_BYTES = 0x04034b50; // https://en.wikipedia.org/wiki/ZIP_(file_format)

        public static bool IsZip(string filename)
        {
            using (var stream = new FileStream(filename, FileMode.Open))
            {
                Debug.Assert(stream != null);
                Debug.Assert(stream.CanSeek);
                stream.Seek(0, 0);

                try
                {
                    byte[] data = new byte[4];

                    stream.Read(data, 0, 4);

                    Debug.Assert(data != null && data.Length >= 4);

                    // if the first 4 bytes of the array are the ZIP signature then it is compressed data
                    return (BitConverter.ToInt32(data, 0) == ZIP_LEAD_BYTES);
                }
                finally
                {
                    stream.Seek(0, 0);  // set the stream back to the begining
                }
            }
        }

        public static byte[] UnZip(string filename)
        {
            MemoryStream ms = new();

            using (var archive = System.IO.Compression.ZipFile.OpenRead(filename))
            {
                foreach (var entry in archive.Entries)
                {
                    using var stream = entry.Open();
                    stream.CopyTo(ms);
                }
            }

            return ms.ToArray();
        }
    }
}
