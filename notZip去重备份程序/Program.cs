class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("备份：输入1；还原：输入2");
        string input = Console.ReadLine();
        int result;

        if (int.TryParse(input, out result))
        {
            switch (result)
            {
                case 1:
                    Console.WriteLine("您选择了备份操作");
                    await BackupManager.PerformBackup();
                    break;
                case 2:
                    Console.WriteLine("您选择了还原操作");
                    await RestoreManager.PerformRestore();
                    break;
                default:
                    Console.WriteLine("无效的选择，请输入1或2");
                    break;
            }
        }
        else
        {
            Console.WriteLine("输入无效，请输入数字1或2");
        }
        Console.ReadKey();
    }
}

